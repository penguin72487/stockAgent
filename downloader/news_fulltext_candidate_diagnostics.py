"""Finite same-response diagnosis through the existing single HTTP/queue owner.

Submitters write only the reviewed private mailbox. The owner reserves a
once-per-candidate observation before dispatch, never changes article heads,
and never retries an uncertain network outcome after a crash. Normal source
work gets worker/provider slots first. Diagnostics cannot accept article data.
"""
from __future__ import annotations

from contextlib import closing
import hashlib
import json
import math
import os
from pathlib import Path
import re
import sqlite3
import stat
import time
from urllib.parse import urlsplit

from downloader.news_fulltext_candidates import native_candidate_source, candidate_response_metadata, candidate_evidence_allowed
from downloader.news_fulltext_candidate_originals import (
    PendingOriginal, QUESTION, LEGACY_QUESTION, legacy_route, private_original_permit,
    private_legacy_original_permit, retain_original, retention_flags, original_route_version,
    BASTILLE_SHORT_QUESTION, ORIGINAL_EVIDENCE_VERSIONS, private_bastille_short_original_permit,
    BASTILLE_CONTINUATION_QUESTION, private_bastille_continuation_original_permit,
    GLOBALVOICES_ORIGINAL_LAYOUT, GLOBALVOICES_ORIGINAL_QUESTION,
    globalvoices_original_url, private_globalvoices_original_permit,
    STOCKFEEL_ORIGINAL_QUESTION, INEWS_ORIGINAL_QUESTION, INEWS_HEADER_ORIGINAL_QUESTION,
    ETTODAY_CREDIT_ORIGINAL_QUESTION, ETTODAY_INLINE_ORIGINAL_QUESTION, ETTODAY_VISIBLE_ORIGINAL_QUESTION,
    ETTODAY_IMAGE_ORIGINAL_QUESTION, ETTODAY_NONEMPTY_ORIGINAL_QUESTION,
    ETTODAY_CANONICAL_ORIGINAL_QUESTION,
)
from downloader.news_fulltext_policy import BASTILLE_LAYOUT_VERSIONS, NewsError, normalize_url
from downloader.news_fulltext_bastillepost import BASTILLE_RESIDENT_EVIDENCE_VERSIONS
from downloader.news_fulltext_setn import (
    INEWS_BOUNDARY_QUESTION, INEWS_LAYOUT_VERSIONS, INEWS_PROVIDER, inews_literal_article_url,
)
from downloader.news_fulltext_ettoday import ETTODAY_BOUNDARY_QUESTION, ettoday_literal_diagnostic_url
from downloader.news_fulltext_pdf import BOARD_PDF_ORIGINAL_LAYOUT, BOARD_PDF_ORIGINAL_QUESTION, private_board_pdf_permit
from downloader.news_fulltext_queue import utc_now
from downloader.news_fulltext_repairs import (
    HEAD_FIELDS, REPAIRABLE, SOURCE_FIELDS, _bytes, _clock, _directory, _head, _head_problem, _pending,
    _quarantine, _read_private_request, _source, _source_contract, _write_once,
)

CONTRACT_VERSION = 1
MAX_PENDING = 16
PUMP_INTERVAL_SECONDS = 5
_KIND = 'reviewed_native_candidate_metadata'
_SAFE_HASH = re.compile(r'[0-9a-f]{64}')
_TERMINAL = frozenset({'observed', 'observed_error', 'skipped', 'uncertain_outcome'})
_CANDIDATE_FAILURES = REPAIRABLE - {'body_not_found'}
_EVIDENCE_VERSIONS = frozenset(range(1, 34))


def _failure_states(layout, evidence):
    # These are diagnoses, NOT normal article requeues. The same exact head,
    # current source, lease/deadline and once-only observation key still bind.
    if evidence in {27, 28, 29, 30, 31, 32, 33}:
        return ({'article_identity_not_verified', 'article_attribution_not_verified'}
                if layout == 'ettoday_visible_article_v39' else set())
    if evidence in {24, 25, 26} or layout in INEWS_LAYOUT_VERSIONS:
        return ({'article_identity_not_verified', 'article_attribution_not_verified'}
                if evidence in {24, 25, 26} and layout in INEWS_LAYOUT_VERSIONS else set())
    if evidence in {20, 21, 22, 23} or layout in {'upmedia_public_modern_article_v1', 'upmedia_public_modern_article_v2', 'upmedia_public_modern_article_v3', 'upmedia_public_modern_article_v4'}:
        return ({'article_identity_not_verified', 'article_attribution_not_verified'}
                if layout == 'upmedia_public_modern_article_v1' and evidence in {20, 21}
                or layout == 'upmedia_public_modern_article_v2' and evidence in {20, 22}
                or layout == 'upmedia_public_modern_article_v3' and evidence in {20, 23}
                or layout == 'upmedia_public_modern_article_v4' and evidence == 20 else set())
    if evidence == 19:
        return {'article_identity_not_verified'} if layout == 'cmnews_visible_article_v1' else set()
    if layout == 'taiwannews_completed_public_original_v3' or evidence == 18:
        return ({'article_identity_not_verified', 'article_title_not_verified'}
                if layout == 'taiwannews_completed_public_original_v3' and evidence == 18 else set())
    if layout == 'taiwannews_completed_public_original_v2' or evidence in {16, 17}:
        if layout == 'taiwannews_completed_public_original_v2' and evidence == 17:
            return {'article_title_not_verified'}
        return ({'article_identity_not_verified', 'article_title_not_verified', 'article_rights_or_scope_excluded'}
                if layout == 'taiwannews_completed_public_original_v2' and evidence == 16 else set())
    if evidence == 15:
        from downloader.news_fulltext_stockfeel import STOCKFEEL_LAYOUT_VERSIONS
        return {'article_identity_not_verified', 'article_attribution_not_verified'} if layout in STOCKFEEL_LAYOUT_VERSIONS else set()
    if evidence == 14:
        return {'article_identity_not_verified', 'article_rights_or_scope_excluded'} if layout in {'stockfeel_visible_original_v1', 'stockfeel_visible_original_v2', 'stockfeel_visible_original_v3', 'stockfeel_visible_original_v4', 'stockfeel_visible_original_v5', 'stockfeel_visible_original_v6', 'stockfeel_visible_original_v7'} else set()
    if layout in {'stockfeel_visible_original_v1', 'stockfeel_visible_original_v2', 'stockfeel_visible_original_v3', 'stockfeel_visible_original_v4', 'stockfeel_visible_original_v5', 'stockfeel_visible_original_v6', 'stockfeel_visible_original_v7'}:
        return set()
    if layout == 'cmnews_visible_article_v1':
        return {'article_identity_not_verified'} if evidence == 1 else set()
    if evidence == 13:
        return {'body_not_found'} if layout == GLOBALVOICES_ORIGINAL_LAYOUT else set()
    if evidence == 12:
        return {'pdf_image_only_or_body_not_found'} if layout == BOARD_PDF_ORIGINAL_LAYOUT else set()
    if evidence in {8, 10} and BASTILLE_LAYOUT_VERSIONS.get(layout, 0) >= 2:
        return {'body_not_found', 'article_identity_not_verified'}
    if evidence == 9 and BASTILLE_LAYOUT_VERSIONS.get(layout, 0) >= 2:
        return {'body_not_found'}
    if evidence == 11:
        return {'body_not_found'} if BASTILLE_LAYOUT_VERSIONS.get(layout, 0) >= 6 else set()
    if evidence == 7:
        return {'unsupported_article_path'}
    if evidence == 5 and original_route_version(layout) == 11:
        return _CANDIDATE_FAILURES | {'unsupported_article_path', 'invalid_bulk_publication_clock'}
    return _CANDIDATE_FAILURES


def _pocket_failure_states(layout, evidence):
    if evidence == 7:
        return {'unsupported_article_path'}
    if evidence == 5 and original_route_version(layout) == 11:
        return {'article_identity_not_verified', 'unsupported_article_path', 'invalid_bulk_publication_clock'}
    return {'article_identity_not_verified'}


def _candidate_head_problem(row, now, *, registry, layout, evidence):
    problem = _head_problem(row, now, registry=registry,
        allowed_failure_states=_failure_states(layout, evidence))
    # Legacy Board PDF failures predate last_checked_at_utc. An unknown OLD
    # check is not a future clock, a body/lease permission, or an unknown new
    # network outcome. Retain NULL and allow only this fixed private-original
    # question; the existing once-only reservation still precedes its GET.
    if (problem == 'missing_or_invalid_check' and evidence == 12
            and layout == BOARD_PDF_ORIGINAL_LAYOUT and row['last_checked_at_utc'] is None):
        return None
    return problem


def _candidate_check_clock(value, evidence):
    return value is None and evidence == 12 or _clock(value) <= time.time()


def _inbox(root):
    return _directory(Path(root), 'candidate_diagnostics', 'pending')


def _key(target, candidate, *, evidence_contract_version=1):
    # User reasons/timestamps and changing failure counters cannot buy a second
    # observation of the same URL and candidate contract.
    if (type(evidence_contract_version) is not int or evidence_contract_version not in _EVIDENCE_VERSIONS
            or evidence_contract_version in {3, 4, 5, 7} and target['provider'] != 'pocket'
            or evidence_contract_version == 6 and target['provider'] != 'watchmedia'
            or evidence_contract_version == 12 and target['provider'] != 'fed_board'
            or evidence_contract_version == 13 and target['provider'] != 'globalvoices'
            or evidence_contract_version == 19 and target['provider'] != 'cmnews'
            or evidence_contract_version in {27, 28, 29, 30, 31, 32, 33} and target['provider'] != 'ettoday'
            or (target['provider'] == INEWS_PROVIDER) != (evidence_contract_version in {24, 25, 26})
            or (target['provider'] == 'upmedia') != (evidence_contract_version in {20, 21, 22, 23})
            or (target['provider'] == 'stockfeel') != (evidence_contract_version in {14, 15})
            or (target['provider'] == 'taiwannews') != (evidence_contract_version in {16, 17, 18})
            or evidence_contract_version in BASTILLE_RESIDENT_EVIDENCE_VERSIONS and target['provider'] != 'bastillepost'):
        raise ValueError('reviewed_candidate_evidence_contract_required')
    basis = [CONTRACT_VERSION, target['provider'], target['id'], candidate]
    if target['provider'] == 'cmnews' and evidence_contract_version == 1:
        basis = [1, target['provider'], target['id'], 'cmnews_current_identity_boundary_v1']
    if evidence_contract_version == 2:
        # A fixed, reviewed NEW question, not an arbitrary reason or hash that
        # could rearm v1. Completed v1 rows/keys/receipts remain unchanged.
        basis = [2, target['provider'], target['id'], candidate, 'videoland_native_credit_clock_v1']
    elif evidence_contract_version == 3:
        basis = [3, target['provider'], target['id'], candidate, 'pocket_native_identity_boundary_v1']
    elif evidence_contract_version == 4:
        # The same failed URL/question cannot be fetched again by promoting a
        # parser, changing its fingerprint or altering a user-supplied reason.
        basis = [4, target['provider'], target['id'], QUESTION]
    elif evidence_contract_version == 5:
        basis = [5, target['provider'], target['id'], LEGACY_QUESTION]
    elif evidence_contract_version == 6:
        basis = [6, target['provider'], target['id'], 'watchmedia_literal_legacy_credit_boundary_v1']
    elif evidence_contract_version == 7:
        from downloader.news_fulltext_pocket_scope import QUESTION as SCOPE_QUESTION
        basis = [7, target['provider'], target['id'], SCOPE_QUESTION]
    elif evidence_contract_version == 8:
        from downloader.news_fulltext_bastillepost import BASTILLE_WRITTEN_BOUNDARY_QUESTION
        basis = [8, target['provider'], target['id'], BASTILLE_WRITTEN_BOUNDARY_QUESTION]
    elif evidence_contract_version == 9:
        basis = [9, target['provider'], target['id'], BASTILLE_SHORT_QUESTION]
    elif evidence_contract_version == 10:
        from downloader.news_fulltext_bastillepost import BASTILLE_ACTIVE_RESOURCE_QUESTION
        basis = [10, target['provider'], target['id'], BASTILLE_ACTIVE_RESOURCE_QUESTION]
    elif evidence_contract_version == 11:
        basis = [11, target['provider'], target['id'], BASTILLE_CONTINUATION_QUESTION]
    elif evidence_contract_version == 12:
        basis = [12, target['provider'], target['id'], BOARD_PDF_ORIGINAL_QUESTION]
    elif evidence_contract_version == 13:
        basis = [13, target['provider'], target['id'], GLOBALVOICES_ORIGINAL_QUESTION]
    elif evidence_contract_version == 14:
        from downloader.news_fulltext_stockfeel import STOCKFEEL_BOUNDARY_QUESTION
        basis = [14, target['provider'], target['id'], STOCKFEEL_BOUNDARY_QUESTION]
    elif evidence_contract_version == 15:
        # Retain one private exact original for a NEW title/byline question.
        # Never rearm the four completed v14 metadata observations.
        basis = [15, target['provider'], target['id'], STOCKFEEL_ORIGINAL_QUESTION]
    elif evidence_contract_version == 16:
        from downloader.news_fulltext_taiwannews import TAIWANNEWS_ORIGINAL_QUESTION
        basis = [16, target['provider'], target['id'], TAIWANNEWS_ORIGINAL_QUESTION]
    elif evidence_contract_version == 17:
        from downloader.news_fulltext_taiwannews import TAIWANNEWS_INLINE_ORIGINAL_QUESTION
        basis = [17, target['provider'], target['id'], TAIWANNEWS_INLINE_ORIGINAL_QUESTION]
    elif evidence_contract_version == 18:
        from downloader.news_fulltext_taiwannews import TAIWANNEWS_SINGLE_ROOT_ORIGINAL_QUESTION
        basis = [18, target['provider'], target['id'], TAIWANNEWS_SINGLE_ROOT_ORIGINAL_QUESTION]
    elif evidence_contract_version == 19:
        from downloader.news_fulltext_publishers import CMNEWS_CANONICAL_RELATION_QUESTION
        basis = [19, target['provider'], target['id'], CMNEWS_CANONICAL_RELATION_QUESTION]
    elif evidence_contract_version == 20:
        from downloader.news_fulltext_upmedia import UPMEDIA_BOUNDARY_QUESTION
        basis = [20, target['provider'], target['id'], UPMEDIA_BOUNDARY_QUESTION]
    elif evidence_contract_version == 21:
        from downloader.news_fulltext_upmedia import UPMEDIA_HEADER_QUESTION
        basis = [21, target['provider'], target['id'], UPMEDIA_HEADER_QUESTION]
    elif evidence_contract_version == 22:
        from downloader.news_fulltext_upmedia import UPMEDIA_EDITOR_QUESTION
        basis = [22, target['provider'], target['id'], UPMEDIA_EDITOR_QUESTION]
    elif evidence_contract_version == 23:
        from downloader.news_fulltext_upmedia import UPMEDIA_REMAINING_QUESTION
        basis = [23, target['provider'], target['id'], UPMEDIA_REMAINING_QUESTION]
    elif evidence_contract_version == 24:
        basis = [24, target['provider'], target['id'], INEWS_BOUNDARY_QUESTION]
    elif evidence_contract_version == 25:
        basis = [25, target['provider'], target['id'], INEWS_ORIGINAL_QUESTION]
    elif evidence_contract_version == 26:
        basis = [26, target['provider'], target['id'], INEWS_HEADER_ORIGINAL_QUESTION]
    elif evidence_contract_version == 27:
        basis = [27, target['provider'], target['id'], ETTODAY_BOUNDARY_QUESTION]
    elif evidence_contract_version == 28:
        basis = [28, target['provider'], target['id'], ETTODAY_CREDIT_ORIGINAL_QUESTION]
    elif evidence_contract_version == 29:
        basis = [29, target['provider'], target['id'], ETTODAY_INLINE_ORIGINAL_QUESTION]
    elif evidence_contract_version == 30:
        basis = [30, target['provider'], target['id'], ETTODAY_VISIBLE_ORIGINAL_QUESTION]
    elif evidence_contract_version == 31:
        basis = [31, target['provider'], target['id'], ETTODAY_IMAGE_ORIGINAL_QUESTION]
    elif evidence_contract_version == 32:
        basis = [32, target['provider'], target['id'], ETTODAY_NONEMPTY_ORIGINAL_QUESTION]
    elif evidence_contract_version == 33:
        basis = [33, target['provider'], target['id'], ETTODAY_CANONICAL_ORIGINAL_QUESTION]
    return hashlib.sha256(_bytes(basis)).hexdigest()


def _prior_unfinished_observation(db, target, *, request_id='', created_at_utc=''):
    # A new metadata question cannot retry another in-flight/unknown operation.
    row = db.execute("SELECT state FROM native_candidate_jobs WHERE provider=? "
        "AND state IN ('ready','reserved','uncertain_outcome') AND request_id<>? "
        "AND json_extract(request_json,'$.target.id')=? "
        "AND (state<>'ready' OR ?='' OR (created_at_utc,request_id)<(?,?)) "
        "ORDER BY CASE state WHEN 'uncertain_outcome' THEN 0 WHEN 'reserved' THEN 1 ELSE 2 END LIMIT 1",
        (target['provider'], request_id, target['id'], request_id, created_at_utc, request_id)).fetchone()
    return row[0] if row else None


def _prior_upmedia_missing_header(db, source, identity, url):
    """A new fixed question needs a completed, same-head missing-field proof.

    This cannot buy a replay of v20 or authorize another observation after an
    uncertain outcome. Recheck at both submission and resident reservation.
    """
    from downloader.news_fulltext_upmedia import UPMEDIA_BOUNDARY_QUESTION
    if not db.execute("SELECT 1 FROM sqlite_master WHERE name='native_candidate_jobs' AND type='table'").fetchone():
        raise ValueError('completed_upmedia_missing_header_proof_required')
    row = db.execute('SELECT * FROM articles WHERE url=?', (url,)).fetchone()
    target = _head(row)
    key = _key(target, source.data_fingerprint, evidence_contract_version=20)
    prior = db.execute('SELECT * FROM native_candidate_jobs WHERE observation_key=?', (key,)).fetchone()
    if (not prior or prior['state'] != 'observed' or not prior['result_json']
            or len(prior['result_json']) > 131072 or len(prior['request_json']) > 131072):
        raise ValueError('completed_upmedia_missing_header_proof_required')
    request, result = json.loads(prior['request_json']), json.loads(prior['result_json'])
    if not isinstance(request, dict) or not isinstance(result, dict):
        raise ValueError('completed_upmedia_missing_header_proof_required')
    boundary = result.get('upmedia_native_boundary', {})
    if (not isinstance(boundary, dict) or request.get('evidence_contract_version') != 20 or request.get('target') != target
            or request.get('source') != identity or request.get('candidate_fingerprint') != source.data_fingerprint
            or result.get('evidence_contract_version') != 20 or result.get('question') != UPMEDIA_BOUNDARY_QUESTION
            or result.get('provider') != 'upmedia' or result.get('requested_url') != url
            or result.get('final_url') != url or result.get('redirect_count') != 0 or result.get('status') != 200
            or result.get('current_native_error') != 'article_attribution_not_verified'
            or result.get('raw_or_names_or_body_retained') is not False
            or boundary.get('byline_links') != 0 or boundary.get('schema_article_nodes') != 1
            or boundary.get('editor_nodes') != 1 or 'publication_header' in boundary):
        raise ValueError('completed_upmedia_missing_header_proof_required')


def _prior_upmedia_editor_geometry(db, source, identity, url):
    """Only missing, distinct fields after an observed current-v2 boundary.

    A newer normal failure may change attempts/state, not the stable URL/head
    identity. Never replay that old question or accept its observation as prose.
    """
    from downloader.news_fulltext_repairs import _unique
    from downloader.news_fulltext_upmedia import UPMEDIA_BOUNDARY_QUESTION, UPMEDIA_LAYOUT_V2
    error = 'completed_upmedia_missing_editor_geometry_proof_required'
    if source.embedded_article_layout != UPMEDIA_LAYOUT_V2 or not db.execute(
            "SELECT 1 FROM sqlite_master WHERE name='native_candidate_jobs' AND type='table'").fetchone():
        raise ValueError(error)
    target = _head(db.execute('SELECT * FROM articles WHERE url=?', (url,)).fetchone())
    key = _key(target, source.data_fingerprint, evidence_contract_version=20)
    prior = db.execute('SELECT * FROM native_candidate_jobs WHERE observation_key=?', (key,)).fetchone()
    if (not prior or prior['state'] != 'observed'
            or any(not isinstance(prior[k], str) or not 0 < len(prior[k]) <= 131072
                   for k in ('request_json', 'result_json'))):
        raise ValueError(error)
    request, result = (json.loads(prior[k], object_pairs_hook=_unique) for k in ('request_json', 'result_json'))
    if not isinstance(request, dict) or not isinstance(result, dict):
        raise ValueError(error)
    old_target, boundary = request.get('target'), result.get('upmedia_native_boundary')
    if (hashlib.sha256(_bytes(request)).hexdigest() != prior['request_id']
            or not isinstance(old_target, dict) or any(old_target.get(k) != target[k] for k in ('id', 'provider', 'url'))
            or request.get('source') != identity or request.get('candidate_layout') != UPMEDIA_LAYOUT_V2
            or request.get('evidence_contract_version') != 20 or request.get('candidate_fingerprint') != source.data_fingerprint
            or result.get('evidence_contract_version') != 20 or result.get('question') != UPMEDIA_BOUNDARY_QUESTION
            or result.get('provider') != 'upmedia' or result.get('requested_url') != url
            or result.get('final_url') != url or result.get('redirect_count') != 0 or result.get('status') != 200
            or result.get('raw_or_names_or_body_retained') is not False or result.get('accepted_as_article') is not False
            or not isinstance(boundary, dict) or boundary.get('contract_version') != 2
            or boundary.get('editor_nodes') != 1 or boundary.get('scope_nodes') != 1
            or 'upmedia_editor_transport' in result
            or not (_clock(prior['created_at_utc']) <= _clock(prior['reserved_at_utc'])
                    <= _clock(prior['completed_at_utc']) <= time.time())):
        raise ValueError(error)


def _prior_upmedia_remaining_context(db, source, identity, url):
    """A completed v2 geometry plus a later real v3 failure, never a replay.

    Only the pinned reviewed source transition can ask for the three absent
    facts. Normal current-parser repair must already have run. Unknown jobs,
    a new reason/parser hash or a failed transport cannot grant this question.
    This is checked again by the resident owner before network reservation.
    """
    from downloader.news_fulltext_repairs import _unique
    from downloader.news_fulltext_upmedia import (
        UPMEDIA_EDITOR_QUESTION, UPMEDIA_LAYOUT_V2, UPMEDIA_LAYOUT_V3, UPMEDIA_V3_HTTP_BINDING)
    error = 'completed_upmedia_missing_remaining_context_proof_required'
    try:
        if (source.embedded_article_layout != UPMEDIA_LAYOUT_V3
                or (source.data_fingerprint, source.authorization_fingerprint) != UPMEDIA_V3_HTTP_BINDING[1:3]
                or any(not db.execute("SELECT 1 FROM sqlite_master WHERE name=? AND type='table'", (table,)).fetchone()
                    for table in ('native_candidate_jobs', 'reviewed_repair_items'))):
            raise ValueError(error)
        target = _head(db.execute('SELECT * FROM articles WHERE url=?', (url,)).fetchone())
        prior = db.execute('SELECT * FROM native_candidate_jobs WHERE observation_key=?',
            (_key(target, UPMEDIA_V3_HTTP_BINDING[0], evidence_contract_version=22),)).fetchone()
        if (not prior or prior['state'] != 'observed' or any(
                not isinstance(prior[k], str) or not 0 < len(prior[k]) <= 131072
                for k in ('request_json', 'result_json'))):
            raise ValueError(error)
        request, result = (json.loads(prior[k], object_pairs_hook=_unique) for k in ('request_json', 'result_json'))
        if not isinstance(request, dict) or not isinstance(result, dict):
            raise ValueError(error)
        old_target, old_source = request.get('target'), request.get('source')
        geometry = result.get('upmedia_editor_transport')
        if (hashlib.sha256(_bytes(request)).hexdigest() != prior['request_id']
                or not isinstance(old_target, dict) or any(old_target.get(k) != target[k] for k in ('id', 'provider', 'url'))
                or not isinstance(old_source, dict) or set(old_source) != SOURCE_FIELDS
                or old_source.get('data_fingerprint') != UPMEDIA_V3_HTTP_BINDING[0]
                or old_source.get('authorization_fingerprint') != UPMEDIA_V3_HTTP_BINDING[2]
                or request.get('candidate_layout') != UPMEDIA_LAYOUT_V2
                or request.get('evidence_contract_version') != 22
                or request.get('candidate_fingerprint') != UPMEDIA_V3_HTTP_BINDING[0]
                or result.get('evidence_contract_version') != 22 or result.get('question') != UPMEDIA_EDITOR_QUESTION
                or result.get('provider') != 'upmedia' or result.get('requested_url') != url
                or result.get('final_url') != url or result.get('redirect_count') != 0 or result.get('status') != 200
                or any(result.get(k) is not False for k in ('raw_or_names_or_body_retained', 'accepted_as_article',
                    'body_completeness_verified', 'identity_or_alias_accepted'))
                or not isinstance(geometry, dict) or geometry.get('contract_version') != 1
                or geometry.get('editor_nodes') != 1 or geometry.get('scope_nodes') != 1
                or 'upmedia_remaining_context' in result or 'remaining_context' in geometry):
            raise ValueError(error)
        marker = db.execute("SELECT recorded_at_utc FROM reviewed_repair_items WHERE article_id=? "
            "AND provider=? AND parser_fingerprint=? AND outcome='enqueued'",
            (bytes.fromhex(target['id']), target['provider'], identity['parser_fingerprint'])).fetchone()
        if (not marker or not (_clock(prior['created_at_utc']) <= _clock(prior['reserved_at_utc'])
                <= _clock(prior['completed_at_utc']) <= _clock(marker['recorded_at_utc'])
                <= _clock(target['last_checked_at_utc']) <= time.time())):
            raise ValueError(error)
    except (KeyError, TypeError, ValueError, RecursionError):
        raise ValueError(error) from None


def _question_source(db, registry, url, evidence):
    if evidence != 7:
        source, identity = _source(db, registry, url)
        if evidence == 12:
            from downloader.news_fulltext_pdf import board_pdf_original_url
            if not board_pdf_original_url(url, source):
                raise ValueError('literal_registered_Board_PDF_required')
        if evidence == 13 and not globalvoices_original_url(url, source):
            raise ValueError('literal_registered_GlobalVoices_article_required')
        if evidence == 17:
            _prior_taiwannews_inline_editor(db, source, url)
        if evidence == 18:
            _prior_taiwannews_single_root(db, source, url)
        if evidence == 19:
            _prior_cmnews_canonical_relation(db, source, identity, url)
        if evidence == 21:
            _prior_upmedia_missing_header(db, source, identity, url)
        if evidence == 22:
            _prior_upmedia_editor_geometry(db, source, identity, url)
        if evidence == 23:
            _prior_upmedia_remaining_context(db, source, identity, url)
        if evidence in {25, 26}:
            _prior_inews_printed_credit(db, source, identity, url, header_credit=evidence == 26)
        if evidence == 28:
            _prior_ettoday_leading_credit(db, source, identity, url)
        if evidence == 29:
            _prior_ettoday_inline_credit(db, source, identity, url)
        if evidence == 30:
            _prior_ettoday_visible_credit(db, source, identity, url)
        if evidence == 31:
            _prior_ettoday_image_credit(db, source, identity, url)
        if evidence == 32:
            _prior_ettoday_nonempty_credit(db, source, identity, url)
        if evidence == 33:
            _prior_ettoday_canonical_credit(db, source, identity, url)
        return source, identity
    from downloader.news_fulltext_pocket_scope import non_school_route
    source = registry.require(url)
    if source.provider != 'pocket' or non_school_route(url) is None:
        raise ValueError('literal_non_school_pocket_route_required')
    return _source_contract(db, source, url)


def _prior_ettoday_inline_credit(db, source, identity, url):
    """A new inline-prefix question, only after the closed plain-line refusal."""
    _prior_ettoday_leading_credit(db,source,identity,url)
    error = 'completed_ettoday_plain_credit_original_scope_refusal_required'
    try:
        target = _head(db.execute('SELECT * FROM articles WHERE url=?',(url,)).fetchone())
        prior = db.execute('SELECT * FROM native_candidate_jobs WHERE observation_key=?',
            (_key(target,source.data_fingerprint,evidence_contract_version=28),)).fetchone()
        if (not prior or prior['state'] != 'observed_error'
                or any(not isinstance(prior[k],str) or not 0 < len(prior[k]) <= 131072
                    for k in ('request_json','result_json'))):
            raise ValueError(error)
        request, result = json.loads(prior['request_json']), json.loads(prior['result_json'])
        if (request['target'] != target or request['source'] != identity or request['evidence_contract_version'] != 28
                or result['error'] != 'private_ettoday_credit_original_scope_not_verified'
                or result['raw_or_names_or_body_retained'] is not False or result['accepted_as_article'] is not False
                or not any(site['module'] == 'news_fulltext_candidate_originals.py'
                    and site['function'] == 'private_ettoday_credit_original_permit'
                    for site in result['failure_location']['sites'])
                or hashlib.sha256(prior['request_json'].encode()).hexdigest() != prior['request_id']
                or not (_clock(prior['created_at_utc']) <= _clock(prior['reserved_at_utc'])
                    <= _clock(prior['completed_at_utc']) <= time.time())):
            raise ValueError(error)
    except (KeyError,IndexError,TypeError,ValueError,RecursionError):
        raise ValueError(error) from None


def _prior_ettoday_visible_credit(db, source, identity, url):
    """Do not replay prefix questions: require the actual closed inline refusal."""
    _prior_ettoday_inline_credit(db,source,identity,url)
    error = 'completed_ettoday_inline_credit_original_scope_refusal_required'
    try:
        target = _head(db.execute('SELECT * FROM articles WHERE url=?',(url,)).fetchone())
        prior = db.execute('SELECT * FROM native_candidate_jobs WHERE observation_key=?',
            (_key(target,source.data_fingerprint,evidence_contract_version=29),)).fetchone()
        if (not prior or prior['state'] != 'observed_error'
                or any(not isinstance(prior[k],str) or not 0 < len(prior[k]) <= 131072
                    for k in ('request_json','result_json'))):
            raise ValueError(error)
        request, result = json.loads(prior['request_json']), json.loads(prior['result_json'])
        if (request['target'] != target or request['source'] != identity or request['evidence_contract_version'] != 29
                or result['error'] != 'private_ettoday_credit_original_scope_not_verified'
                or result['raw_or_names_or_body_retained'] is not False or result['accepted_as_article'] is not False
                or not any(site['module'] == 'news_fulltext_candidate_originals.py'
                    and site['function'] == 'private_ettoday_credit_original_permit'
                    for site in result['failure_location']['sites'])
                or hashlib.sha256(prior['request_json'].encode()).hexdigest() != prior['request_id']
                or not (_clock(prior['created_at_utc']) <= _clock(prior['reserved_at_utc'])
                    <= _clock(prior['completed_at_utc']) <= time.time())):
            raise ValueError(error)
    except (KeyError,IndexError,TypeError,ValueError,RecursionError):
        raise ValueError(error) from None


def _prior_ettoday_image_credit(db, source, identity, url):
    """Actual leading image refusal, not a speculative request for another tag."""
    _prior_ettoday_visible_credit(db,source,identity,url)
    error = 'completed_ettoday_visible_prefix_image_refusal_required'
    try:
        target = _head(db.execute('SELECT * FROM articles WHERE url=?',(url,)).fetchone())
        prior = db.execute('SELECT * FROM native_candidate_jobs WHERE observation_key=?',
            (_key(target,source.data_fingerprint,evidence_contract_version=30),)).fetchone()
        if (not prior or prior['state'] != 'observed'
                or any(not isinstance(prior[k],str) or not 0 < len(prior[k]) <= 131072
                    for k in ('request_json','result_json'))):
            raise ValueError(error)
        request, result = json.loads(prior['request_json']), json.loads(prior['result_json'])
        if (request['target'] != target or request['source'] != identity or request['evidence_contract_version'] != 30
                or result['private_original_scope_error'] != 'private_ettoday_credit_original_scope_not_verified'
                or result['visible_credit_scope'] != {'verification_stage':'visible_prefix','prefix_nodes':1,
                    'comment_nodes':0,'tag_counts':{'img':1},'separator_found':False}
                or result['raw_or_names_or_body_retained'] is not False or result['accepted_as_article'] is not False
                or hashlib.sha256(prior['request_json'].encode()).hexdigest() != prior['request_id']
                or not (_clock(prior['created_at_utc']) <= _clock(prior['reserved_at_utc'])
                    <= _clock(prior['completed_at_utc']) <= time.time())):
            raise ValueError(error)
    except (KeyError,IndexError,TypeError,ValueError,RecursionError):
        raise ValueError(error) from None


def _prior_ettoday_nonempty_credit(db, source, identity, url):
    """The exact closed image/empty-line observation, not another tag guess."""
    _prior_ettoday_image_credit(db,source,identity,url)
    error = 'completed_ettoday_image_empty_line_refusal_required'
    try:
        target = _head(db.execute('SELECT * FROM articles WHERE url=?',(url,)).fetchone())
        prior = db.execute('SELECT * FROM native_candidate_jobs WHERE observation_key=?',
            (_key(target,source.data_fingerprint,evidence_contract_version=31),)).fetchone()
        if (not prior or prior['state'] != 'observed'
                or any(not isinstance(prior[k],str) or not 0 < len(prior[k]) <= 131072
                    for k in ('request_json','result_json'))):
            raise ValueError(error)
        request, result = json.loads(prior['request_json']), json.loads(prior['result_json'])
        expected = {'verification_stage':'printed_reporter','prefix_nodes':2,'comment_nodes':0,
            'tag_counts':{'img':1,'br':1},'separator_found':True,'prefix_characters':0,
            'prefix_sha256':hashlib.sha256(b'').hexdigest(),'following_visible_text':True}
        if (request['target'] != target or request['source'] != identity or request['evidence_contract_version'] != 31
                or result['private_original_scope_error'] != 'private_ettoday_credit_original_scope_not_verified'
                or result['visible_credit_scope'] != expected
                or result['raw_or_names_or_body_retained'] is not False or result['accepted_as_article'] is not False
                or hashlib.sha256(prior['request_json'].encode()).hexdigest() != prior['request_id']
                or not (_clock(prior['created_at_utc']) <= _clock(prior['reserved_at_utc'])
                    <= _clock(prior['completed_at_utc']) <= time.time())):
            raise ValueError(error)
    except (KeyError,IndexError,TypeError,ValueError,RecursionError):
        raise ValueError(error) from None


def _prior_ettoday_canonical_credit(db, source, identity, url):
    """Reuse the reviewed author contract only after the closed strict refusal.

    This is not a fresh retry of a failed request or a metadata-to-author
    upgrade. The actual visible reporter line and both clocks already passed;
    the new question concerns the existing adapter's author relationship.
    """
    _prior_ettoday_nonempty_credit(db,source,identity,url)
    error = 'completed_ettoday_nonempty_author_relationship_refusal_required'
    try:
        target = _head(db.execute('SELECT * FROM articles WHERE url=?',(url,)).fetchone())
        prior = db.execute('SELECT * FROM native_candidate_jobs WHERE observation_key=?',
            (_key(target,source.data_fingerprint,evidence_contract_version=32),)).fetchone()
        if (not prior or prior['state'] != 'observed'
                or any(not isinstance(prior[k],str) or not 0 < len(prior[k]) <= 131072
                    for k in ('request_json','result_json'))):
            raise ValueError(error)
        request, result = json.loads(prior['request_json']), json.loads(prior['result_json'])
        trace = result['visible_credit_scope']
        if (request['target'] != target or request['source'] != identity or request['evidence_contract_version'] != 32
                or result['question'] != ETTODAY_NONEMPTY_ORIGINAL_QUESTION
                or result['private_original_scope_error'] != 'article_attribution_not_verified'
                or trace['verification_stage'] != 'same_article_person'
                or trace['separator_found'] is not True or trace['following_visible_text'] is not True
                or type(trace['leading_blank_lines']) is not int or not 1 <= trace['leading_blank_lines'] <= 128
                or type(trace['prefix_characters']) is not int or not 1 <= trace['prefix_characters'] <= 160
                or not _SAFE_HASH.fullmatch(trace['prefix_sha256'])
                or result['status'] != 200 or result['requested_url'] != url or result['final_url'] != url
                or result['redirect_count'] != 0 or result['raw_or_names_or_body_retained'] is not False
                or result['accepted_as_article'] is not False or 'private_original' in result
                or 'private_original_permit' in result
                or hashlib.sha256(prior['request_json'].encode()).hexdigest() != prior['request_id']
                or not (_clock(prior['created_at_utc']) <= _clock(prior['reserved_at_utc'])
                    <= _clock(prior['completed_at_utc']) <= time.time())):
            raise ValueError(error)
    except (KeyError,IndexError,TypeError,ValueError,RecursionError):
        raise ValueError(error) from None


def _prior_ettoday_leading_credit(db, source, identity, url):
    """Current same-head v27 lead, not a replay or metadata-to-author upgrade."""
    error = 'completed_ettoday_leading_reporter_break_proof_required'
    try:
        if not db.execute("SELECT 1 FROM sqlite_master WHERE name='native_candidate_jobs'").fetchone():
            raise ValueError(error)
        target = _head(db.execute('SELECT * FROM articles WHERE url=?',(url,)).fetchone())
        key = _key(target, source.data_fingerprint, evidence_contract_version=27)
        prior = db.execute('SELECT * FROM native_candidate_jobs WHERE observation_key=?',(key,)).fetchone()
        if (not prior or prior['state'] != 'observed'
                or any(not isinstance(prior[k], str) or not 0 < len(prior[k]) <= 131072
                       for k in ('request_json','result_json'))):
            raise ValueError(error)
        request, facts = json.loads(prior['request_json']), json.loads(prior['result_json'])
        boundary = facts['ettoday_native_boundary']
        story = boundary['story_credit_anatomy'][0]
        first = story['paragraph_shapes'][0]
        if (source.provider != 'ettoday' or request['source'] != identity or request['target'] != target
                or request['evidence_contract_version'] != 27 or facts['evidence_contract_version'] != 27
                or facts['question'] != ETTODAY_BOUNDARY_QUESTION or facts['provider'] != 'ettoday'
                or facts['requested_url'] != url or facts['final_url'] != url
                or facts['redirect_count'] != 0 or facts['status'] != 200
                or facts['accepted_as_article'] is not False or facts['raw_or_names_or_body_retained'] is not False
                or boundary['native_story_count'] != 1 or len(boundary['story_credit_anatomy']) != 1
                or story['paragraph_scan_truncated'] or story['candidate_rows_truncated']
                or not boundary['canonical_equals_literal_request'] or not boundary['og_url_equals_literal_request']
                or boundary['schema_error'] or boundary['metadata_truncated']
                or boundary['article_schema_count'] != 1 or boundary['declared_author_types'] != [['Person']]
                or boundary['schema_identity_relations'] != [{'url_equals_literal_request':True,
                    'main_entity_equals_literal_request':True,'headline_equals_single_native_heading':True}]
                or first['direct_paragraph_index'] != 0 or not first['direct_child'] or not first['visible']
                or not first['embedded_reporter_signature'] or not first['embedded_signature_starts_at_zero']
                or first['exact_reporter_signature'] or first['line_breaks'] < 1 or first['reporter_count'] != 1
                or not _SAFE_HASH.fullmatch(first['text_sha256'])
                or hashlib.sha256(prior['request_json'].encode()).hexdigest() != prior['request_id']
                or not (_clock(prior['created_at_utc']) <= _clock(prior['reserved_at_utc'])
                        <= _clock(prior['completed_at_utc']) <= time.time())):
            raise ValueError(error)
    except (KeyError, IndexError, TypeError, ValueError, RecursionError):
        raise ValueError(error) from None


def _prior_inews_printed_credit(db, source, identity, url, *, header_credit=False):
    """Completed same-head credit geometry, never a replay of v24 or v25."""
    error = ('completed_inews_named_header_editor_proof_required' if header_credit
        else 'completed_inews_empty_header_printed_paragraph_proof_required')
    try:
        if not db.execute("SELECT 1 FROM sqlite_master WHERE name='native_candidate_jobs'").fetchone():
            raise ValueError(error)
        target = _head(db.execute('SELECT * FROM articles WHERE url=?', (url,)).fetchone())
        key = _key(target, source.data_fingerprint, evidence_contract_version=24)
        prior = db.execute('SELECT * FROM native_candidate_jobs WHERE observation_key=?', (key,)).fetchone()
        if (not prior or prior['state'] != 'observed'
                or any(not isinstance(prior[k], str) or not 0 < len(prior[k]) <= 131072
                       for k in ('request_json', 'result_json'))):
            raise ValueError(error)
        request, facts = json.loads(prior['request_json']), json.loads(prior['result_json'])
        boundary = facts['inews_native_boundary']
        credits, editors = boundary['credits'], boundary['editors']
        if header_credit:
            from downloader.news_fulltext_setn import INEWS_V4_HTTP_BINDING, INEWS_V5_HTTP_BINDING
            old = request['source']
            # v4's completed geometry remains evidence after the exact v5
            # credit-only promotion. It grants no retry or new HTTP identity.
            lineage = (source.data_fingerprint == INEWS_V5_HTTP_BINDING[0]
                and source.authorization_fingerprint == INEWS_V5_HTTP_BINDING[1]
                and set(old) == SOURCE_FIELDS and old['contract_version'] == identity['contract_version']
                and old['data_fingerprint'] == INEWS_V4_HTTP_BINDING[0]
                and old['authorization_fingerprint'] == INEWS_V4_HTTP_BINDING[1]
                and _SAFE_HASH.fullmatch(old['parser_fingerprint'])
                and (source.http_capacity_source_fingerprint, source.http_capacity_authorization_fingerprint)
                    == INEWS_V5_HTTP_BINDING[2:])
            links = credits[0]['links']
            credit_ok = (len(credits) == 1 and credits[0]['prefix_candidate'] == '記者'
                and not credits[0]['element']['hidden'] and len(links) == 1
                and links[0]['native_author'] is True and not links[0]['hidden']
                and not links[0]['child_tags'] and 2 <= links[0]['characters'] <= 20
                and _SAFE_HASH.fullmatch(links[0]['text_sha256']))
        else:
            lineage = False
            paragraph = editors[0]['bodies'][0]['children'][0]
            credit_ok = (len(credits) == 1 and credits[0]['prefix_candidate'] == 'empty'
                and credits[0]['element']['characters'] == 0 and not credits[0]['links']
                and paragraph['tag'] == 'p' and 1 <= paragraph['characters'] <= 160
                and not paragraph['child_tags'] and _SAFE_HASH.fullmatch(paragraph['text_sha256']))
        if (source.provider != INEWS_PROVIDER or not (request['source'] == identity or lineage) or request['target'] != target
                or request['evidence_contract_version'] != 24 or facts['evidence_contract_version'] != 24
                or facts['question'] != INEWS_BOUNDARY_QUESTION or facts['provider'] != INEWS_PROVIDER
                or facts['requested_url'] != url or facts['final_url'] != url
                or facts['redirect_count'] != 0 or facts['status'] != 200
                or facts['accepted_as_article'] is not False or facts['raw_or_names_or_body_retained'] is not False
                or not credit_ok or len(editors) != 1 or editors[0]['body_count'] != 1
                or hashlib.sha256(prior['request_json'].encode()).hexdigest() != prior['request_id']
                or not (_clock(prior['created_at_utc']) <= _clock(prior['reserved_at_utc'])
                        <= _clock(prior['completed_at_utc']) <= time.time())):
            raise ValueError(error)
    except (KeyError, IndexError, TypeError, ValueError, RecursionError):
        raise ValueError(error) from None


def _prior_cmnews_canonical_relation(db, source, identity, url):
    """Only the unchanged completed counts-only canonical-mismatch witness.

    This asks which literal URL was declared; it cannot repeat identity-v1,
    resolve an unknown network outcome, or manufacture a new candidate key.
    """
    try:
        row = db.execute('SELECT * FROM articles WHERE url=?', (url,)).fetchone()
        target = _head(row)
        prior = db.execute('SELECT * FROM native_candidate_jobs WHERE observation_key=?',
            (_key(target, source.data_fingerprint),)).fetchone()
        if prior is None or prior['state'] != 'observed':
            raise ValueError('completed observation required')
        request, facts = json.loads(prior['request_json']), json.loads(prior['result_json'])
        native = facts['native_identity']
        if (source.provider != 'cmnews' or request['source'] != identity or request['target'] != target
                or request.get('evidence_contract_version', 1) != 1
                or request['candidate_fingerprint'] != source.data_fingerprint
                or request['candidate_layout'] != 'cmnews_visible_article_v1'
                or facts['provider'] != 'cmnews' or facts['question'] != 'cmnews_current_identity_boundary_v1'
                or facts['current_native_error'] != 'article_identity_not_verified'
                or facts['accepted_as_article'] is not False or facts['raw_or_names_or_body_retained'] is not False
                or facts['body_completeness_verified'] is not False
                or native['canonical_count'] != 1 or native['matching_canonical_count'] != 0
                or native['matching_native_scope_count'] != 1
                or native['native_slug_matches'] is not True or native['native_UUID_container_matches'] is not True
                or native['native_title_count'] != 1 or native['nonempty_native_title_count'] != 1
                or native['native_body_container_count'] != 1):
            raise ValueError('unchanged canonical-mismatch witness required')
    except (KeyError, TypeError, ValueError, sqlite3.OperationalError):
        raise ValueError('prior_completed_cmnews_canonical_mismatch_required') from None
    if recorded_cmnews_canonical(db, source, target):
        raise ValueError('cmnews_literal_canonical_already_recorded')


def recorded_cmnews_canonical(db, source, target):
    """Reuse exact prior failure metadata before asking a new HTTP question.

    Only the failed check's UTC day and preceding day can contain its receipt
    (HTTP receipt and queue finish may cross midnight). No full-store walk,
    body decode, article acceptance, alias join or archive mutation is needed.
    The display URL must hash to the original literal, excluding truncation
    and query/fragment redaction. Source identity and exact request also bind.
    """
    from datetime import UTC, datetime, timedelta
    main = next((Path(row[2]) for row in db.execute('PRAGMA database_list') if row[1] == 'main' and row[2]), None)
    if main is None or main.name != 'queue.sqlite3' or source.provider != 'cmnews':
        return None
    base = main.parent/'identity_metadata'
    checked = datetime.fromtimestamp(_clock(target['last_checked_at_utc']), UTC)
    for day in (checked, checked-timedelta(days=1)):
        for path in sorted((base/day.strftime('%Y%m%d')/'cmnews').glob('*.json')):
            try:
                if not path.resolve().is_relative_to(base.resolve()):
                    continue
                with os.fdopen(os.open(path, os.O_RDONLY | getattr(os, 'O_NOFOLLOW', 0)
                        | getattr(os, 'O_NONBLOCK', 0)), 'rb') as stream:
                    info = os.fstat(stream.fileno())
                    if not stat.S_ISREG(info.st_mode) or info.st_size > 65536 or info.st_mode & 0o077:
                        continue
                    payload = stream.read(65537)
                facts = json.loads(payload)
                if (facts.get('contract_version') != 1 or facts.get('provider') != 'cmnews'
                        or facts.get('data_fingerprint') != source.data_fingerprint
                        or facts.get('authorization_fingerprint') != source.authorization_fingerprint
                        or facts.get('request_url') != target['url'] or facts.get('final_url') != target['url']
                        or facts.get('request_url_sha256') != target['id'] or facts.get('final_url_sha256') != target['id']
                        or facts.get('error_code') != 'article_identity_not_verified' or facts.get('http_status') != 200
                        or facts.get('diagnostic_only') is not True or facts.get('raw_response_retained') is not False
                        or facts.get('accepted_as_article') is not False
                        or _clock(facts.get('observed_at_utc')) > checked.timestamp()):
                    continue
                canonical = facts['facts']['canonical']
                if canonical['node_count'] != 1 or canonical['truncated'] is not False or len(canonical['values']) != 1:
                    continue
                value = canonical['values'][0]; literal = value['display_url']
                parts = urlsplit(literal)
                if (not isinstance(literal, str) or not 1 <= len(literal) <= 2048
                        or parts.scheme not in {'http','https'} or not parts.hostname or parts.username or parts.password
                        or parts.query or parts.fragment
                        or hashlib.sha256(literal.encode()).hexdigest() != value['original_value_sha256']):
                    continue
                return {'canonical_url': literal, 'metadata_path': str(path),
                    'metadata_sha256': hashlib.sha256(payload).hexdigest(),
                    'observed_at_utc': facts['observed_at_utc'], 'article_or_alias_accepted': False}
            except (OSError, KeyError, TypeError, ValueError):
                continue
    return None


def _prior_taiwannews_inline_editor(db, source, url):
    """A new legacy-scope question needs an actual completed v16 witness.

    This does not rearm v16, accept its title/author or retry an unknown GET.
    Only literal editor+minute suffix metadata, not a bare publisher label,
    can justify observing that still-unaccepted legacy original once.
    """
    from downloader.news_fulltext_taiwannews import TAIWANNEWS_ORIGINAL_QUESTION, taiwannews_evidence_source
    target = {'provider': source.provider, 'id': hashlib.sha256(url.encode()).hexdigest()}
    if (not taiwannews_evidence_source(source)
            or not db.execute("SELECT 1 FROM sqlite_master WHERE name='native_candidate_jobs' AND type='table'").fetchone()):
        raise ValueError('prior_literal_inline_editor_evidence_required')
    row = db.execute('SELECT state,request_json,result_json FROM native_candidate_jobs WHERE observation_key=?',
        (_key(target, source.data_fingerprint, evidence_contract_version=16),)).fetchone()
    try:
        request = json.loads(row['request_json']) if row else {}
        result = json.loads(row['result_json']) if row else {}
        native = result['taiwannews_native_boundary']
        fields = native['declarations']
        published, visible = fields['published'], fields['visible_clock']
        if not (row['state'] == 'observed' and request['evidence_contract_version'] == 16
                and request['candidate_fingerprint'] == source.data_fingerprint and request['target']['url'] == url
                and result['question'] == TAIWANNEWS_ORIGINAL_QUESTION and result['current_native_error'] == 'article_title_not_verified'
                and result['accepted_as_article'] is False and result['raw_or_names_or_body_retained'] is False
                and native['editor_count'] == fields['native_heading']['count'] == 1
                and published['count'] == visible['count'] == 1 and len(published['values']) == len(visible['values']) == 1):
            raise ValueError('prior_literal_inline_editor_evidence_required')
        minute = published['values'][0]['value']
        if visible['values'][0]['value'] != '台灣英文新聞－編輯 ' + minute:
            raise ValueError('prior_literal_inline_editor_evidence_required')
    except (KeyError, TypeError, ValueError):
        raise ValueError('prior_literal_inline_editor_evidence_required') from None


def _prior_taiwannews_single_root(db, source, url):
    """Exact completed17 format witness, not a retry of that closed question.

    Only the unchanged failed head with literal inline credit and exactly the
    pinned root-completion instruction/no moves may ask this new question.
    Unknown, retained, changed-source or unrelated failures are not evidence.
    Recheck on claim as well as submission; never mutate the prior row.
    """
    from downloader.news_fulltext_taiwannews import (
        TAIWANNEWS_INLINE_ORIGINAL_QUESTION, TAIWANNEWS_V2_LAYOUT,
        TAIWANNEWS_V3_HTTP_BINDING, _RC_INSTRUCTION_SHA256, _native_clock, taiwannews_resident_source)
    target = {'provider': source.provider, 'id': hashlib.sha256(url.encode()).hexdigest()}
    if (not taiwannews_resident_source(source)
            or not db.execute("SELECT 1 FROM sqlite_master WHERE name='native_candidate_jobs' AND type='table'").fetchone()):
        raise ValueError('prior_completed_single_root_evidence_required')
    row = db.execute('SELECT state,request_json,result_json FROM native_candidate_jobs WHERE observation_key=?',
        (_key(target, source.data_fingerprint, evidence_contract_version=17),)).fetchone()
    current = db.execute('SELECT * FROM articles WHERE url=?', (url,)).fetchone()
    try:
        request = json.loads(row['request_json']) if row else {}
        result = json.loads(row['result_json']) if row else {}
        native = result['taiwannews_native_boundary']
        fields = native['declarations']
        published, visible = fields['published'], fields['visible_clock']
        if not (row['state'] == 'observed' and request['evidence_contract_version'] == 17
                and request['candidate_layout'] == TAIWANNEWS_V2_LAYOUT
                and request['candidate_fingerprint'] == TAIWANNEWS_V3_HTTP_BINDING[0]
                and request['source']['data_fingerprint'] == TAIWANNEWS_V3_HTTP_BINDING[0]
                and request['source']['authorization_fingerprint'] == source.authorization_fingerprint
                and current is not None and request['target'] == _head(current)
                and result['question'] == TAIWANNEWS_INLINE_ORIGINAL_QUESTION
                and result['current_native_error'] == 'article_identity_not_verified' and result['status'] == 200
                and result['accepted_as_article'] is False and result['raw_or_names_or_body_retained'] is False
                and native['editor_count'] == fields['native_heading']['count'] == 1
                and native['literal_moves'] == [] and native['stream_scripts'] == {
                    'count': 1, 'sha256': [_RC_INSTRUCTION_SHA256]}
                and published['count'] == visible['count'] == 1 and len(published['values']) == len(visible['values']) == 1):
            raise ValueError('prior_completed_single_root_evidence_required')
        minute = _native_clock(published['values'][0]['value'])
        if visible['values'][0]['value'] != '台灣英文新聞－編輯 ' + minute:
            raise ValueError('prior_completed_single_root_evidence_required')
    except (KeyError, TypeError, ValueError, NewsError):
        raise ValueError('prior_completed_single_root_evidence_required') from None


def submit_candidate(root, config, registry, url, layout, *, reason, evidence_contract_version=1):
    if not isinstance(reason, str) or not reason.strip() or len(reason) > 240:
        raise ValueError('candidate_requires_bounded_reason')
    if not isinstance(url, str) or normalize_url(url) != url:
        raise ValueError('literal_candidate_url_required')
    root = Path(root).resolve()
    candidate = native_candidate_source(config, registry, registry.base, layout)
    if not candidate_evidence_allowed(registry.sources[candidate.provider], candidate, evidence_contract_version):
        raise ValueError('reviewed_candidate_evidence_contract_required')
    if evidence_contract_version in {24, 25, 26} and not inews_literal_article_url(url):
        raise ValueError('literal_current_inews_route_required')
    if evidence_contract_version == 27 and not ettoday_literal_diagnostic_url(url,
            allow_house='house.ettoday.net' in candidate.reviewed_article_subsites):
        raise ValueError('literal_current_ettoday_route_required')
    if candidate.provider == 'watchmedia':
        from downloader.news_fulltext_watchmedia import watchmedia_article_id
        is_legacy = watchmedia_article_id(url, requested=True) == 'literal_legacy_route'
        if is_legacy != (evidence_contract_version == 6):
            raise ValueError('watchmedia_literal_legacy_requires_fixed_evidence6')
    if evidence_contract_version in {3, 4}:
        from downloader.news_fulltext_pocket import pocket_article_id
        if pocket_article_id(url, adapter_contract_version=(
                original_route_version(layout) if evidence_contract_version == 4 else 1)) is None:
            raise ValueError('literal_current_pocket_canonical_route_required')
    if evidence_contract_version == 5 and not legacy_route(url, adapter_contract_version=original_route_version(layout)):
        raise ValueError('literal_current_pocket_legacy_route_required')
    if evidence_contract_version == 7:
        from downloader.news_fulltext_pocket_scope import non_school_route
        if non_school_route(url) is None:
            raise ValueError('literal_non_school_pocket_route_required')
    if evidence_contract_version in BASTILLE_RESIDENT_EVIDENCE_VERSIONS:
        from downloader.news_fulltext_bastillepost import bastillepost_article_id
        version = BASTILLE_LAYOUT_VERSIONS[layout] if evidence_contract_version in {10, 11} else 2
        if bastillepost_article_id(url, requested=True, adapter_contract_version=version) is None:
            raise ValueError('literal_current_bastillepost_route_required')
    with closing(sqlite3.connect((root/'queue.sqlite3').as_uri()+'?mode=ro', uri=True)) as db:
        db.row_factory = sqlite3.Row
        db.execute('BEGIN')
        row = db.execute('SELECT * FROM articles WHERE url=?', (url,)).fetchone()
        states = _failure_states(layout, evidence_contract_version)
        problem = _candidate_head_problem(row, time.time(), registry=registry, layout=layout,
            evidence=evidence_contract_version)
        if problem:
            raise ValueError(problem)
        if row['state'] not in states:
            raise ValueError('reviewed_validation_failure_required')
        if evidence_contract_version in {3, 4, 5, 7} and row['state'] not in _pocket_failure_states(layout, evidence_contract_version):
            raise ValueError('reviewed_pocket_identity_failure_required')
        source, identity = _question_source(db, registry, url, evidence_contract_version)
        if source.provider != row['provider'] or source.provider != candidate.provider:
            raise ValueError('candidate_provider_mismatch')
        target = _head(row)
        key = _key(target, candidate.data_fingerprint, evidence_contract_version=evidence_contract_version)
        if db.execute("SELECT 1 FROM sqlite_master WHERE name='native_candidate_jobs' AND type='table'").fetchone():
            if evidence_contract_version in {2, 6}:
                prior = db.execute('SELECT state FROM native_candidate_jobs WHERE observation_key=?',
                    (_key(target, candidate.data_fingerprint),)).fetchone()
                if prior and prior['state'] in {'ready', 'reserved', 'uncertain_outcome'}:
                    raise ValueError('prior_observation_not_terminal')
            existing = db.execute('SELECT request_id,state FROM native_candidate_jobs WHERE observation_key=?', (key,)).fetchone()
            if existing:
                return {'contract_version': CONTRACT_VERSION, 'submitted': False,
                    'request_id': existing['request_id'], 'state': existing['state'],
                    'publisher_requests': 0, 'article_heads_mutated': False, 'accepted_as_article': False}
            if (candidate.provider == 'cmnews' or evidence_contract_version in {
                    7, 12, 13, 14, 15, 18, 20, 21, 22, 23, 24, 25, 26, 27, *BASTILLE_RESIDENT_EVIDENCE_VERSIONS}) and _prior_unfinished_observation(db, target):
                raise ValueError('prior_observation_not_terminal')
    request = {'contract_version': CONTRACT_VERSION, 'kind': _KIND,
        'created_at_utc': utc_now(), 'reason': reason.strip(), 'target': target, 'source': identity,
        'candidate_layout': layout, 'candidate_fingerprint': candidate.data_fingerprint, 'observation_key': key}
    if evidence_contract_version >= 2:
        request.update(contract_version=evidence_contract_version, evidence_contract_version=evidence_contract_version)
    directory = _inbox(root)
    paths = _pending(directory)
    for path in paths:
        try:
            previous = _load(path)
        except (OSError, ValueError, TypeError, KeyError, OverflowError, RecursionError):
            continue  # Only the owner may quarantine bad control files.
        if previous['observation_key'] == key:
            return {'contract_version': CONTRACT_VERSION, 'submitted': False, 'request_id': path.stem,
                'state': 'pending', 'publisher_requests': 0, 'article_heads_mutated': False, 'accepted_as_article': False}
    if len(paths) >= MAX_PENDING:
        raise ValueError('candidate_inbox_full')
    payload = _bytes(request)
    digest = hashlib.sha256(payload).hexdigest()
    _write_once(directory/(digest+'.json'), payload)
    return {'contract_version': CONTRACT_VERSION, 'submitted': True, 'request_id': digest,
        'state': 'pending', 'publisher_requests': 0, 'article_heads_mutated': False, 'accepted_as_article': False}


def _load(path):
    request = _read_private_request(path)
    fields = {'contract_version', 'kind', 'created_at_utc', 'reason', 'target', 'source',
              'candidate_layout', 'candidate_fingerprint', 'observation_key'}
    version = request.get('contract_version') if isinstance(request, dict) else None
    evidence = request.get('evidence_contract_version', 1) if isinstance(request, dict) else None
    if type(version) is int and version in _EVIDENCE_VERSIONS - {1}:
        fields.add('evidence_contract_version')
    if (not isinstance(request, dict) or set(request) != fields
            or type(version) is not int or version not in _EVIDENCE_VERSIONS
            or type(evidence) is not int or evidence != version
            or request['kind'] != _KIND or _clock(request['created_at_utc']) > time.time()
            or not isinstance(request['reason'], str) or not request['reason'].strip() or len(request['reason']) > 240
            or not isinstance(request['target'], dict) or set(request['target']) != set(HEAD_FIELDS)
            or not isinstance(request['source'], dict) or set(request['source']) != SOURCE_FIELDS
            or type(request['source']['contract_version']) is not int
            or request['source']['contract_version'] < 1
            or any(not isinstance(request['source'][key], str) or not _SAFE_HASH.fullmatch(request['source'][key])
                   for key in SOURCE_FIELDS - {'contract_version'})
            or not isinstance(request['candidate_layout'], str) or not (
                request['candidate_layout'] == BOARD_PDF_ORIGINAL_LAYOUT
                or request['candidate_layout'] in INEWS_LAYOUT_VERSIONS
                or request['candidate_layout'] in {'upmedia_public_modern_article_v1', 'upmedia_public_modern_article_v2', 'upmedia_public_modern_article_v3', 'upmedia_public_modern_article_v4'}
                or request['candidate_layout'] in {'taiwannews_completed_public_original_v2', 'taiwannews_completed_public_original_v3'}
                or request['candidate_layout'] == GLOBALVOICES_ORIGINAL_LAYOUT
                or request['candidate_layout'] in {'stockfeel_visible_original_v1', 'stockfeel_visible_original_v2', 'stockfeel_visible_original_v3', 'stockfeel_visible_original_v4', 'stockfeel_visible_original_v5', 'stockfeel_visible_original_v6', 'stockfeel_visible_original_v7'}
                or BASTILLE_LAYOUT_VERSIONS.get(request['candidate_layout'], 0) >= 2 or re.fullmatch(
                    r'(?:cmnews_visible_article_v1|pocket_visible_article_v[0-9]{1,2}|videoland_visible_article_v4|watchmedia_visible_article_v3|ettoday_visible_article_v39|max_visible_post_v[0-9]{1,2})',
                    request['candidate_layout']))
            or not isinstance(request['candidate_fingerprint'], str) or not _SAFE_HASH.fullmatch(request['candidate_fingerprint'])
            or not isinstance(request['observation_key'], str)):
        raise ValueError('invalid_native_candidate_request')
    target = request['target']
    if (not isinstance(target['url'], str) or normalize_url(target['url']) != target['url']
            or target['id'] != hashlib.sha256(target['url'].encode()).hexdigest()
            or not isinstance(target['provider'], str) or not re.fullmatch(r'[a-z][a-z0-9_]{0,79}', target['provider'])
            or target['state'] not in _failure_states(request['candidate_layout'], evidence) or target['error_code'] != target['state']
            or type(target['attempts']) is not int or target['attempts'] < 0
            or type(target['due']) not in (int, float) or not math.isfinite(target['due'])
            or not 0 <= target['due'] <= time.time() or not _candidate_check_clock(target['last_checked_at_utc'], evidence)
            or evidence == 2 and (target['provider'] != 'videoland'
                or request['candidate_layout'] != 'videoland_visible_article_v4')
            or request['candidate_layout'] == 'cmnews_visible_article_v1' and (
                target['provider'] != 'cmnews' or evidence not in {1, 19})
            or evidence == 19 and (target['provider'] != 'cmnews' or request['candidate_layout'] != 'cmnews_visible_article_v1')
            or evidence in {27, 28, 29, 30, 31, 32, 33} and (target['provider'] != 'ettoday' or request['candidate_layout'] != 'ettoday_visible_article_v39')
            or request['candidate_layout'] == 'ettoday_visible_article_v39' and evidence not in {1, 27, 28, 29, 30, 31, 32, 33}
            or (request['candidate_layout'] in INEWS_LAYOUT_VERSIONS) != (evidence in {24, 25, 26})
            or evidence in {24, 25, 26} and (target['provider'] != INEWS_PROVIDER or not inews_literal_article_url(target['url']))
            or request['candidate_layout'] == 'watchmedia_visible_article_v3' and (
                target['provider'] != 'watchmedia' or evidence not in {1, 6})
            or evidence == 6 and (target['provider'] != 'watchmedia'
                or request['candidate_layout'] != 'watchmedia_visible_article_v3')
            or (request['candidate_layout'] == BOARD_PDF_ORIGINAL_LAYOUT) != (evidence == 12)
            or evidence == 12 and target['provider'] != 'fed_board'
            or (request['candidate_layout'] == GLOBALVOICES_ORIGINAL_LAYOUT) != (evidence == 13)
            or evidence == 13 and target['provider'] != 'globalvoices'
            or (request['candidate_layout'] in {'stockfeel_visible_original_v1', 'stockfeel_visible_original_v2', 'stockfeel_visible_original_v3', 'stockfeel_visible_original_v4', 'stockfeel_visible_original_v5', 'stockfeel_visible_original_v6', 'stockfeel_visible_original_v7'}) != (evidence in {14, 15})
            or evidence in {14, 15} and target['provider'] != 'stockfeel'
            or (request['candidate_layout'] in {'upmedia_public_modern_article_v1', 'upmedia_public_modern_article_v2', 'upmedia_public_modern_article_v3', 'upmedia_public_modern_article_v4'}) != (evidence in {20, 21, 22, 23})
            or request['candidate_layout'] == 'upmedia_public_modern_article_v1' and evidence not in {20, 21}
            or request['candidate_layout'] == 'upmedia_public_modern_article_v2' and evidence not in {20, 22}
            or request['candidate_layout'] == 'upmedia_public_modern_article_v3' and evidence not in {20, 23}
            or request['candidate_layout'] == 'upmedia_public_modern_article_v4' and evidence != 20
            or evidence in {20, 21, 22, 23} and target['provider'] != 'upmedia'
            or (request['candidate_layout'] == 'taiwannews_completed_public_original_v2') != (evidence in {16, 17})
            or (request['candidate_layout'] == 'taiwannews_completed_public_original_v3') != (evidence == 18)
            or evidence in {16, 17, 18} and target['provider'] != 'taiwannews'
            or (BASTILLE_LAYOUT_VERSIONS.get(request['candidate_layout'], 0) >= 2) != (evidence in BASTILLE_RESIDENT_EVIDENCE_VERSIONS)
            or evidence in BASTILLE_RESIDENT_EVIDENCE_VERSIONS and target['provider'] != 'bastillepost'
            or (request['candidate_layout'] == 'pocket_visible_article_v3') != (evidence == 3)
            or evidence in {3, 4, 5, 7} and (target['provider'] != 'pocket'
                or target['state'] not in _pocket_failure_states(request['candidate_layout'], evidence))
            or evidence in {4, 5, 7} and (not re.fullmatch(r'pocket_visible_article_v[0-9]{1,2}', request['candidate_layout'])
                or int(request['candidate_layout'].rsplit('v', 1)[1]) < 5)
            or request['candidate_layout'].startswith('pocket_') and evidence not in {3, 4, 5, 7}
            or request['observation_key'] != _key(target, request['candidate_fingerprint'],
                evidence_contract_version=evidence)):
        raise ValueError('invalid_native_candidate_target')
    if evidence in {3, 4}:
        from downloader.news_fulltext_pocket import pocket_article_id
        if pocket_article_id(target['url'], adapter_contract_version=(
                original_route_version(request['candidate_layout']) if evidence == 4 else 1)) is None:
            raise ValueError('literal_current_pocket_canonical_route_required')
    if evidence in {27, 28, 29, 30, 31, 32, 33} and not ettoday_literal_diagnostic_url(target['url'], allow_house=True):
        # Host review is also checked against the current registry at claim.
        raise ValueError('literal_current_ettoday_route_required')
    if evidence == 5 and not legacy_route(target['url'], adapter_contract_version=original_route_version(request['candidate_layout'])):
        raise ValueError('literal_current_pocket_legacy_route_required')
    if evidence == 6:
        from downloader.news_fulltext_watchmedia import watchmedia_article_id
        if watchmedia_article_id(target['url'], requested=True) != 'literal_legacy_route':
            raise ValueError('watchmedia_literal_legacy_requires_fixed_evidence6')
    if evidence == 7:
        from downloader.news_fulltext_pocket_scope import non_school_route
        if non_school_route(target['url']) is None:
            raise ValueError('literal_non_school_pocket_route_required')
    if evidence in BASTILLE_RESIDENT_EVIDENCE_VERSIONS:
        from downloader.news_fulltext_bastillepost import bastillepost_article_id
        version = BASTILLE_LAYOUT_VERSIONS[request['candidate_layout']] if evidence in {10, 11} else 2
        if bastillepost_article_id(target['url'], requested=True, adapter_contract_version=version) is None:
            raise ValueError('literal_current_bastillepost_route_required')
    return request


def _schema(queue):
    if getattr(queue, 'native_candidate_schema_ready', False):
        return
    with queue.db:
        queue.db.execute('CREATE TABLE IF NOT EXISTS native_candidate_jobs('
            'request_id TEXT PRIMARY KEY,observation_key TEXT UNIQUE NOT NULL,provider TEXT NOT NULL,'
            'state TEXT NOT NULL,request_json TEXT NOT NULL,result_json TEXT,'
            'created_at_utc TEXT NOT NULL,reserved_at_utc TEXT,completed_at_utc TEXT,owner_id TEXT NOT NULL)')
        queue.db.execute('CREATE INDEX IF NOT EXISTS native_candidate_ready ON native_candidate_jobs(state,provider,created_at_utc)')
        # A dispatch reservation is the unknown-outcome barrier, not permission
        # to send again. Committed results are archived below without any GET.
        queue.db.execute("UPDATE native_candidate_jobs SET state='uncertain_outcome',completed_at_utc=? "
            "WHERE state='reserved' AND owner_id<>?", (utc_now(), queue.owner))
    queue.native_candidate_schema_ready = True


def _archive(queue, path, job, *, duplicate_of=None):
    result = json.loads(job['result_json']) if job['result_json'] else None
    outcome = {'contract_version': CONTRACT_VERSION, 'request_id': path.stem,
        'observation_request_id': job['request_id'], 'observation_key': job['observation_key'],
        'provider': job['provider'], 'state': job['state'], 'owner_id': job['owner_id'],
        'created_at_utc': job['created_at_utc'], 'reserved_at_utc': job['reserved_at_utc'],
        'completed_at_utc': job['completed_at_utc'], 'result': result,
        'duplicate_of': duplicate_of, 'accepted_as_article': False, 'raw_or_names_or_body_retained': False,
        'article_heads_mutated': False, 'publish': False, 'remote_training_export': False}
    outcome.update(retention_flags(json.loads(job['request_json']), result, job['state']))
    directory = _directory(queue.root, 'candidate_diagnostics', 'processed')
    _write_once(directory/(path.stem+'.outcome.json'), _bytes(outcome))
    archived = directory/path.name
    if archived.is_symlink() or archived.exists() and hashlib.sha256(archived.read_bytes()).hexdigest() != path.stem:
        raise ValueError('candidate_archive_conflict')
    os.replace(path, archived)
    for parent in (directory, path.parent):
        descriptor = os.open(parent, os.O_RDONLY | getattr(os, 'O_DIRECTORY', 0))
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)


def pump_candidates(queue, *, stop=None, max_seconds=0.05):
    """At most16 private requests per tick; no article tree or body scan."""
    if type(max_seconds) not in (int, float) or not math.isfinite(max_seconds) or not 0 < max_seconds <= 0.1:
        raise ValueError('invalid_candidate_slice_budget')
    started = time.monotonic()
    base = queue.root/'reviewed_repairs'/'candidate_diagnostics'
    if not base.exists() and not base.is_symlink():
        return
    try:
        paths = _pending(_inbox(queue.root))[:MAX_PENDING]
        _schema(queue)
    except (OSError, ValueError):
        queue.metrics.count('local_repairs', 'candidate_inbox_unavailable')
        return
    for path in paths:
        if time.monotonic()-started >= max_seconds or stop is not None and stop.is_set():
            break
        try:
            request = _load(path)
            provider = request['target']['provider']
            if (provider not in queue.registry.sources or not queue.registry.sources[provider].allowed
                    or not queue.registry.sources[provider].request_enabled):
                raise ValueError('candidate_provider_no_longer_allowed')
        except (OSError, ValueError, TypeError, KeyError, OverflowError, RecursionError) as error:
            try:
                _quarantine(queue, path, error)
            except (OSError, ValueError):
                queue.metrics.count('local_repairs', 'candidate_quarantine_deferred')
            continue
        with queue.db:
            queue.db.execute('INSERT OR IGNORE INTO native_candidate_jobs '
                '(request_id,observation_key,provider,state,request_json,created_at_utc,owner_id) VALUES (?,?,?,?,?,?,?)',
                (path.stem, request['observation_key'], request['target']['provider'], 'ready',
                 _bytes(request).decode(), request['created_at_utc'], queue.owner))
        job = queue.db.execute('SELECT * FROM native_candidate_jobs WHERE observation_key=?',
            (request['observation_key'],)).fetchone()
        if job['state'] in _TERMINAL:
            try:
                _archive(queue, path, job, duplicate_of=job['request_id'] if job['request_id'] != path.stem else None)
            except (OSError, ValueError):
                queue.metrics.count('local_repairs', 'candidate_archive_deferred')
    queue.native_candidate_ready_providers = {row[0] for row in queue.db.execute(
        "SELECT DISTINCT provider FROM native_candidate_jobs WHERE state='ready'")}


def has_unadmitted_candidates(queue):
    """Wake an idle owner for NEW input, not for an already-paused ready job."""
    directory = queue.root/'reviewed_repairs'/'candidate_diagnostics'/'pending'
    try:
        for parent in (directory.parent.parent, directory.parent, directory):
            mode = parent.lstat().st_mode
            if not stat.S_ISDIR(mode) or mode & 0o077:
                return False
        names = [path.stem for path in _pending(directory)[:MAX_PENDING]]
    except OSError:
        return False
    if not names:
        return False
    if not getattr(queue, 'native_candidate_schema_ready', False):
        return True
    registered = {row[0] for row in queue.db.execute(
        'SELECT request_id FROM native_candidate_jobs WHERE request_id IN ('+','.join('?' for _ in names)+')', names)}
    return any(name not in registered for name in names)


def claim_candidate(queue, http, config, provider):
    if (provider not in getattr(queue, 'native_candidate_ready_providers', ())
            or http.provider_not_before.get(provider, 0) > time.time()):
        return None
    for job in queue.db.execute("SELECT * FROM native_candidate_jobs WHERE state='ready' AND provider=? "
            'ORDER BY created_at_utc LIMIT ?', (provider, MAX_PENDING)).fetchall():
        request = json.loads(job['request_json'])
        target = request['target']
        evidence_version = request.get('evidence_contract_version', 1)
        parts = urlsplit(target['url'])
        if (parts.netloc in http.blocked_gateway_hosts(provider)
                or parts.scheme+'://'+parts.netloc in http.blocked_robots_origins(provider)):
            continue
        try:
            if evidence_version in {7, 12, 13, 14, 15, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, *BASTILLE_RESIDENT_EVIDENCE_VERSIONS}:
                # Pending files can race before either enters SQLite. Serialize
                # ready questions in a stable order; otherwise they would each
                # see the other as "prior" and wait forever. Reservations and
                # uncertain outcomes always block, regardless of creation order.
                prior = _prior_unfinished_observation(queue.db, target, request_id=job['request_id'],
                    created_at_utc=job['created_at_utc'])
                if prior == 'uncertain_outcome':
                    raise ValueError('prior_observation_uncertain')
                if prior:
                    continue
            if evidence_version == 2:
                prior = queue.db.execute('SELECT state FROM native_candidate_jobs WHERE observation_key=?',
                    (_key(target, request['candidate_fingerprint']),)).fetchone()
                if prior and prior['state'] in {'ready', 'reserved'}:
                    continue
                if prior and prior['state'] == 'uncertain_outcome':
                    raise ValueError('prior_observation_uncertain')
            row = queue.db.execute('SELECT * FROM articles WHERE id=?', (bytes.fromhex(target['id']),)).fetchone()
            problem = _candidate_head_problem(row, time.time(), registry=queue.registry,
                layout=request['candidate_layout'], evidence=evidence_version)
            if problem or _head(row) != target:
                raise ValueError('head_changed_or_protected')
            source, identity = _question_source(queue.db, queue.registry, target['url'], evidence_version)
            if evidence_version in {27, 28, 29, 30, 31, 32, 33} and not ettoday_literal_diagnostic_url(target['url'],
                    allow_house='house.ettoday.net' in source.reviewed_article_subsites):
                raise ValueError('literal_current_ettoday_route_required')
            if source.provider != provider or identity != request['source']:
                raise ValueError('source_identity_changed')
            candidate = native_candidate_source(config, queue.registry, queue.registry.base, request['candidate_layout'])
            if candidate.provider != provider or candidate.data_fingerprint != request['candidate_fingerprint']:
                raise ValueError('candidate_identity_changed')
            if not candidate_evidence_allowed(source, candidate, evidence_version):
                raise ValueError('reviewed_candidate_evidence_contract_required')
        except (ValueError, NewsError) as error:
            with queue.db:
                queue.db.execute("UPDATE native_candidate_jobs SET state='skipped',completed_at_utc=?,owner_id=?,result_json=? "
                    'WHERE request_id=?', (utc_now(), queue.owner,
                        _bytes({'reason': error.code if isinstance(error, NewsError) else 'head_or_source_no_longer_eligible'}).decode(), job['request_id']))
            continue
        # The same main-thread writer owns reservation and future completion.
        # Reserve durably before handing any HTTP operation to the executor.
        with queue.db:
            queue.db.execute("UPDATE native_candidate_jobs SET state='reserved',reserved_at_utc=?,owner_id=? "
                "WHERE request_id=? AND state='ready'", (utc_now(), queue.owner, job['request_id']))
        return {'request_id': job['request_id'], 'provider': provider, 'url': target['url'],
            'source': source, 'candidate': candidate, 'evidence_contract_version': evidence_version}
    return None


def fetch_candidate(http, job):
    evidence_version = job.get('evidence_contract_version', 1)
    if (job['provider'] != job['source'].provider
            or not candidate_evidence_allowed(job['source'], job['candidate'], evidence_version)):
        raise ValueError('reviewed_candidate_evidence_contract_required')
    started = time.perf_counter()
    response = http.get(job['url'])
    received = time.perf_counter()
    if evidence_version == 6:
        from downloader.news_fulltext_watchmedia import watchmedia_article_id
        hops = response.redirects
        if (watchmedia_article_id(job['url'], requested=True) != 'literal_legacy_route'
                or watchmedia_article_id(response.url) is None or len(hops) != 1
                or hops[0].status != 301 or hops[0].from_url != job['url']
                or hops[0].to_url != response.url or hops[0].location != response.url):
            raise NewsError('candidate_literal_legacy_response_not_verified')
    elif evidence_version not in {5, 7, 16, 17, 18, *BASTILLE_RESIDENT_EVIDENCE_VERSIONS} and (response.url != job['url'] or response.redirects):
        raise NewsError('candidate_literal_response_not_verified')
    facts = candidate_response_metadata(response, job['source'], job['candidate'],
        evidence_contract_version=evidence_version,
        **({'requested_url':job['url']} if evidence_version in {5, 6, 7, 12, 13, 14, 15, 16, 17, 18, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, *BASTILLE_RESIDENT_EVIDENCE_VERSIONS} else {}))
    parsed = time.perf_counter()
    permit = None
    if evidence_version in {15, 16, 17, 18, 25, 26, 28, 29, 30, 31, 32, 33}:
        permit = facts.pop('private_original_permit', None)
    current_errors = {'article_identity_not_verified'}
    if evidence_version == 5 and original_route_version(job['source'].embedded_article_layout) == 11:
        current_errors.add('invalid_bulk_publication_clock')
    if evidence_version in {4, 5} and facts['current_error'] in current_errors:
        try:
            permit = (private_legacy_original_permit(response,job['source'],job['url'])
                if evidence_version == 5 else private_original_permit(response,job['source']))
        except NewsError as error:
            from downloader.news_fulltext_candidates import extraction_failure_location
            facts['private_original_scope_error'] = 'private_original_scope_not_verified'
            facts['private_original_scope_failure_location'] = extraction_failure_location(error)
    if evidence_version in {9, 11}:
        try:
            permit = (private_bastille_short_original_permit if evidence_version == 9
                else private_bastille_continuation_original_permit)(response, job['source'], job['url'])
        except NewsError as error:
            facts['private_original_scope_error'] = error.code
    if evidence_version == 12:
        permit = private_board_pdf_permit(response, job['source'], job['url'])
    if evidence_version == 13:
        permit = private_globalvoices_original_permit(response, job['source'], job['url'])
    if evidence_version == 5 and permit is None and response.status == 200 and len(response.body) <= 2 << 20:
        from bs4 import BeautifulSoup
        from downloader.news_fulltext_pocket import pocket_identity_metadata
        # A scope refusal still has finite identity facts, never raw/body text.
        facts['pocket_native_validation'] = pocket_identity_metadata(
            BeautifulSoup(response.body,job['source'].html_parser),page_url=response.url)
    finished = time.perf_counter()
    facts['timing'] = {'http_seconds': received-started, 'parse_seconds': parsed-received,
        'scope_seconds': finished-parsed, 'total_seconds': finished-started}
    if permit is not None:
        return PendingOriginal(response, facts, permit)
    return facts


def finish_candidate(queue, job, *, result=None, error=None):
    original_result = isinstance(result, PendingOriginal)
    retention_uncertain = False
    if original_result and error is None:
        # Check ownership BEFORE a sidecar can be created. A failed local write
        # consumes this observation and does not kill/restart the HTTP owner.
        reserved = queue.db.execute("SELECT 1 FROM native_candidate_jobs WHERE request_id=? "
            "AND state='reserved' AND owner_id=?", (job['request_id'], queue.owner)).fetchone()
        if not reserved or job.get('evidence_contract_version') not in ORIGINAL_EVIDENCE_VERSIONS:
            raise ValueError('candidate_completion_owner_mismatch')
        try:
            if (not isinstance(result.facts, dict) or result.facts.get('accepted_as_article') is not False
                    or result.facts.get('raw_or_names_or_body_retained') is not False
                    or len(_bytes(result.facts)) > 60000):
                raise ValueError('finite_candidate_metadata_required')
            write_started = time.perf_counter()
            result = retain_original(queue, job, result)
            result['original_write_readback_seconds'] = time.perf_counter()-write_started
        except (NewsError, OSError, ValueError, TypeError, OverflowError, RecursionError) as failure:
            error, retention_uncertain = failure, True
    if error is not None:
        code = error.code if isinstance(error, NewsError) else 'worker_'+type(error).__name__
        if not isinstance(code, str) or not re.fullmatch(r'[A-Za-z0-9_]{1,96}', code):
            code = 'unclassified_worker_error'
        result, state = {'error': code, 'accepted_as_article': False, 'raw_or_names_or_body_retained': False}, 'observed_error'
        if isinstance(error, NewsError):
            # A private-original refusal used to discard its parser site,
            # forcing another source request merely to locate the gate.
            # Reuse the bounded code-local ABI: no source text, arbitrary
            # message, absolute path or traceback locals enter the receipt.
            from downloader.news_fulltext_candidates import extraction_failure_location
            result.update(error_diagnostic_contract_version=1,
                failure_location=extraction_failure_location(error))
        if retention_uncertain:
            result.update(raw_or_names_or_body_retained=None, raw_retention_state='unknown')
    else:
        state = 'observed'
    try:
        if (not isinstance(result, dict) or result.get('accepted_as_article') is not False
                or result.get('raw_or_names_or_body_retained') is not False
                and not (original_result and job.get('evidence_contract_version') in ORIGINAL_EVIDENCE_VERSIONS
                    and (result.get('raw_or_names_or_body_retained') is True or retention_uncertain))):
            raise ValueError('finite_candidate_metadata_required')
        payload = _bytes(result)
    except (TypeError, ValueError, OverflowError, RecursionError):
        payload, state = _bytes({'error': 'invalid_candidate_result', 'accepted_as_article': False,
                                'raw_or_names_or_body_retained': False}), 'observed_error'
    if len(payload) > 65536:
        payload, state = _bytes({'error': 'candidate_result_exceeds_bound', 'accepted_as_article': False,
                                'raw_or_names_or_body_retained': False}), 'observed_error'
    with queue.db:
        cursor = queue.db.execute('UPDATE native_candidate_jobs SET state=?,result_json=?,completed_at_utc=? '
            "WHERE request_id=? AND state='reserved' AND owner_id=?",
            (state, payload.decode(), utc_now(), job['request_id'], queue.owner))
        if cursor.rowcount != 1:
            raise ValueError('candidate_completion_owner_mismatch')
    queue.metrics.count(job['provider'], 'native_candidate_'+state)


def candidate_status(root, request_id):
    """Private read-only status; never acquire a news/HTTP owner lock."""
    if not isinstance(request_id, str) or not _SAFE_HASH.fullmatch(request_id):
        raise ValueError('invalid_candidate_request_id')
    root = Path(root).resolve()
    flags = {'contract_version': CONTRACT_VERSION, 'accepted_as_article': False,
             'raw_or_names_or_body_retained': False, 'article_heads_mutated': False,
             'publish': False, 'remote_training_export': False}
    with closing(sqlite3.connect((root/'queue.sqlite3').as_uri()+'?mode=ro', uri=True)) as db:
        db.row_factory = sqlite3.Row
        if db.execute("SELECT 1 FROM sqlite_master WHERE name='native_candidate_jobs' AND type='table'").fetchone():
            row = db.execute('SELECT request_id,provider,state,created_at_utc,reserved_at_utc,completed_at_utc,result_json,request_json '
                'FROM native_candidate_jobs WHERE request_id=?', (request_id,)).fetchone()
            if row:
                result = json.loads(row['result_json']) if row['result_json'] else None
                return dict(flags, **retention_flags(json.loads(row['request_json']), result, row['state']),
                    **{key: result if key == 'result_json' else value
                       for key, value in dict(row).items() if key != 'request_json'})
    path = root/'reviewed_repairs'/'candidate_diagnostics'/'pending'/(request_id+'.json')
    if path.exists():
        _load(path)
        return dict(flags, request_id=request_id, state='pending', publisher_requests=0)
    return dict(flags, request_id=request_id, state='unknown')
