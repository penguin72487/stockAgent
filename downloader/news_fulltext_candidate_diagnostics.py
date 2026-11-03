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
from downloader.news_fulltext_diagnostics import IDENTITY_METADATA_READ_VERSIONS
from downloader.news_fulltext_candidate_originals import (
    PendingOriginal, QUESTION, LEGACY_QUESTION, legacy_route, private_original_permit,
    private_legacy_original_permit, retain_original, retention_flags, original_route_version,
    BASTILLE_SHORT_QUESTION, ORIGINAL_EVIDENCE_VERSIONS, private_bastille_short_original_permit,
    BASTILLE_CONTINUATION_QUESTION, private_bastille_continuation_original_permit,
    GLOBALVOICES_ORIGINAL_LAYOUT, GLOBALVOICES_ORIGINAL_QUESTION,
    globalvoices_original_url, private_globalvoices_original_permit,
    STOCKFEEL_ORIGINAL_QUESTION, INEWS_ORIGINAL_QUESTION, INEWS_HEADER_ORIGINAL_QUESTION,
    INEWS_BODY_ORIGINAL_QUESTION,
    INEWS_SHORT_CREDIT_ORIGINAL_QUESTION,
    ETTODAY_CREDIT_ORIGINAL_QUESTION, ETTODAY_INLINE_ORIGINAL_QUESTION, ETTODAY_VISIBLE_ORIGINAL_QUESTION,
    ETTODAY_IMAGE_ORIGINAL_QUESTION, ETTODAY_NONEMPTY_ORIGINAL_QUESTION,
    ETTODAY_CANONICAL_ORIGINAL_QUESTION,
    ETTODAY_CONFLICT_ORIGINAL_QUESTION,
    GVM_ORIGINAL_QUESTION,
)
from downloader.news_fulltext_policy import BASTILLE_LAYOUT_VERSIONS, NewsError, REQUEST_DEFERRAL_CODES, normalize_url
from downloader.news_fulltext_bastillepost import BASTILLE_RESIDENT_EVIDENCE_VERSIONS
from downloader.news_fulltext_contentplatform import CONTENTPLATFORM_EVIDENCE_QUESTIONS, CONTENTPLATFORM_EVIDENCE_VERSIONS
from downloader.news_fulltext_setn import (
    INEWS_BOUNDARY_QUESTION, INEWS_LAYOUT_VERSIONS, INEWS_PROVIDER, inews_literal_article_url,
)
from downloader.news_fulltext_ettoday import (
    ETTODAY_BOUNDARY_QUESTION, ETTODAY_PRELUDE_EXCERPT_QUESTION, ETTODAY_ATTRIBUTION_PREDICATES_QUESTION,
    ettoday_literal_diagnostic_url,
)
from downloader.news_fulltext_pdf import BOARD_PDF_ORIGINAL_LAYOUT, BOARD_PDF_ORIGINAL_QUESTION, private_board_pdf_permit
from downloader.news_fulltext_queue import utc_now
from downloader.news_fulltext_repairs import (
    HEAD_FIELDS, REPAIRABLE, SOURCE_FIELDS, _bytes, _clock, _directory, _head, _head_problem, _pending,
    _record_turn, _rotation_order,
    _quarantine, _read_private_request, _source, _source_contract, _unsafe_request_path, _write_once,
)

CONTRACT_VERSION = 1
CANDIDATE_DEFERRAL_CONTRACT_VERSION = 1
MAX_PENDING = 16
PUMP_INTERVAL_SECONDS = 5
_CANDIDATE_CURSOR_KEY = 'native_candidate_pump_cursor_v1'
_LOCAL_RETRY_INITIAL_SECONDS = 1.0
_LOCAL_RETRY_MAX_SECONDS = 30.0
_KIND = 'reviewed_native_candidate_metadata'
_SAFE_HASH = re.compile(r'[0-9a-f]{64}')
_TERMINAL = frozenset({'observed', 'observed_error', 'skipped', 'uncertain_outcome'})
_CANDIDATE_FAILURES = REPAIRABLE - {'body_not_found'}
_EVIDENCE_VERSIONS = frozenset(range(1, 69))


class _ETtodayInlinePredecessorError(ValueError):
    """Keep the public guard error stable and expose only a finite private cause."""

    _DETAILS = frozenset({'state', 'payload', 'identity', 'result', 'site', 'hash', 'clock', 'malformed'})

    def __init__(self, detail):
        if detail not in self._DETAILS:
            raise ValueError('invalid_predecessor_guard_detail')
        super().__init__('completed_ettoday_plain_credit_original_scope_refusal_required')
        self.detail = detail
        # Python's traceback renders notes while str(error) stays compatible.
        # This also diagnoses submit-time failures, before a job can exist.
        self.add_note('ettoday_inline_predecessor_guard=' + detail)


def _failure_states(layout, evidence):
    # These are diagnoses, NOT normal article requeues. The same exact head,
    # current source, lease/deadline and once-only observation key still bind.
    if evidence == 67:
        from downloader.news_fulltext_moneyweekly_excerpt import LAYOUT
        return {'unsupported_article_path', 'body_not_found'} if layout == LAYOUT else set()
    if evidence == 66:
        from downloader.news_fulltext_moneyweekly_metadata import LAYOUT
        return {'unsupported_article_path', 'body_not_found'} if layout == LAYOUT else set()
    if evidence == 65:
        from downloader.news_fulltext_moneyweekly import BODY_GEOMETRY_LAYOUT
        return {'unsupported_article_path', 'body_not_found'} if layout == BODY_GEOMETRY_LAYOUT else set()
    if evidence == 64:
        from downloader.news_fulltext_moneyweekly import ROUTE_METADATA_LAYOUT
        return {'unsupported_article_path'} if layout == ROUTE_METADATA_LAYOUT else set()
    if evidence in {61, 62, 63, 68}:
        return {'article_attribution_not_verified'} if layout == 'ettoday_visible_article_v41' else set()
    if evidence == 60:
        return {'article_attribution_not_verified'} if layout == 'ettoday_visible_article_v40' else set()
    if evidence == 59:
        return {'article_identity_not_verified'} if layout=='manager_public_written_article_v5' else set()
    if evidence == 58:
        return {'body_not_found'} if layout == 'manager_public_written_article_v4' else set()
    if evidence == 57:
        from downloader.news_fulltext_knews import KNEWS_GRID_NATIVE_LAYOUT
        return {'article_identity_not_verified'} if layout == KNEWS_GRID_NATIVE_LAYOUT else set()
    if evidence == 56:
        from downloader.news_fulltext_knews import KNEWS_SPARSE_NATIVE_LAYOUT, KNEWS_TABLE_NATIVE_LAYOUT, KNEWS_GRID_NATIVE_LAYOUT, KNEWS_MULTI_REPORTER_LAYOUT
        return ({'article_identity_not_verified', 'article_attribution_not_verified'}
                if layout in {KNEWS_SPARSE_NATIVE_LAYOUT, KNEWS_TABLE_NATIVE_LAYOUT, KNEWS_GRID_NATIVE_LAYOUT, KNEWS_MULTI_REPORTER_LAYOUT} else set())
    if evidence == 55:
        from downloader.news_fulltext_contentplatform import CONTENTPLATFORM_WRITTEN_HTML_LAYOUT
        return {'article_identity_not_verified'} if layout == CONTENTPLATFORM_WRITTEN_HTML_LAYOUT else set()
    if evidence == 50:
        return {'awaiting_authorization', 'body_not_found'} if layout == 'gnn_native_legacy_article_v4' else set()
    if evidence in {48,49} or layout in {'gnn_native_reporter_article_v1', 'gnn_native_credited_article_v2', 'gnn_native_written_article_v3', 'gnn_native_legacy_article_v4'}:
        return set(REPAIRABLE) if evidence in {48,49} and layout in {'gnn_native_reporter_article_v1', 'gnn_native_credited_article_v2', 'gnn_native_written_article_v3', 'gnn_native_legacy_article_v4'} else set()
    if evidence in {46,47} or layout in {'manager_public_written_article_v1', 'manager_public_written_article_v2', 'manager_public_written_article_v3', 'manager_public_written_article_v4', 'manager_public_written_article_v5'}:
        return (set(REPAIRABLE) | {'article_rights_or_scope_excluded'}
                if evidence in {46,47} and layout in {'manager_public_written_article_v1', 'manager_public_written_article_v2', 'manager_public_written_article_v3', 'manager_public_written_article_v4', 'manager_public_written_article_v5'} else set())
    from downloader.news_fulltext_gvm import GVM_CANDIDATE_LAYOUTS
    from downloader.news_fulltext_contentplatform import CONTENTPLATFORM_ORIGINAL_LAYOUTS
    if evidence in CONTENTPLATFORM_EVIDENCE_VERSIONS or layout in CONTENTPLATFORM_ORIGINAL_LAYOUTS:
        return {'article_identity_not_verified'} if evidence in CONTENTPLATFORM_EVIDENCE_VERSIONS and layout in CONTENTPLATFORM_ORIGINAL_LAYOUTS else set()
    if evidence in GVM_CANDIDATE_LAYOUTS or layout in GVM_CANDIDATE_LAYOUTS.values():
        if GVM_CANDIDATE_LAYOUTS.get(evidence)!=layout:return set()
        return ({'article_identity_not_verified','article_rights_or_scope_excluded'}
                if evidence==34 else {'article_identity_not_verified'})
    if evidence in {38, 39}:
        return {'body_not_found'} if layout == 'setn_inews_native_article_v8' else set()
    if evidence in {27, 28, 29, 30, 31, 32, 33, 51, 52}:
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
    if evidence in {19,54}:
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
    if evidence in {65, 66, 67} and row is not None and row['state'] not in _failure_states(layout, evidence):
        return 'not_semantic_parser_failure'
    problem = _head_problem(row, now, registry=registry,
        allowed_failure_states=_failure_states(layout, evidence))
    # Legacy Board PDF failures predate last_checked_at_utc. An unknown OLD
    # check is not a future clock, a body/lease permission, or an unknown new
    # network outcome. Retain NULL and allow only this fixed private-original
    # question; the existing once-only reservation still precedes its GET.
    if (problem == 'missing_or_invalid_check' and evidence == 12
            and layout == BOARD_PDF_ORIGINAL_LAYOUT and row['last_checked_at_utc'] is None):
        return None
    if (problem == 'missing_or_invalid_check' and evidence in {64, 65, 66, 67}
            and row['state'] == 'unsupported_article_path' and row['last_checked_at_utc'] is None):
        # Path exclusion can be classified without an article GET. Its NULL
        # clock is unknown, not a future check or a failed source observation.
        # This fixed first-hop question still requires the exact unhandled
        # route, body-free unleased head, elapsed deadline and once-only key.
        return None
    return problem


def _candidate_check_clock(value, evidence):
    return value is None and evidence in {12, 64, 65, 66, 67} or _clock(value) <= time.time()


def _inbox(root):
    return _directory(Path(root), 'candidate_diagnostics', 'pending')


def _key(target, candidate, *, evidence_contract_version=1):
    from downloader.news_fulltext_gvm import GVM_CANDIDATE_LAYOUTS
    # User reasons/timestamps and changing failure counters cannot buy a second
    # observation of the same URL and candidate contract.
    if (type(evidence_contract_version) is not int or evidence_contract_version not in _EVIDENCE_VERSIONS
            or (target['provider'] == 'moneyweekly') != (evidence_contract_version in {64, 65, 66, 67})
            or (target['provider'] == 'knews') != (evidence_contract_version in {56, 57})
            or (target['provider'] == 'gnn') != (evidence_contract_version in {48,49,50})
            or (target['provider'] == 'managertoday') != (evidence_contract_version in {46,47,58,59})
            or evidence_contract_version in {3, 4, 5, 7} and target['provider'] != 'pocket'
            or evidence_contract_version == 6 and target['provider'] != 'watchmedia'
            or evidence_contract_version == 12 and target['provider'] != 'fed_board'
            or evidence_contract_version == 13 and target['provider'] != 'globalvoices'
            or evidence_contract_version in {19,54} and target['provider'] != 'cmnews'
            or (target['provider'] == 'contentplatform') != (evidence_contract_version in CONTENTPLATFORM_EVIDENCE_VERSIONS or evidence_contract_version == 55)
            or (target['provider'] == 'gvm') != (evidence_contract_version in GVM_CANDIDATE_LAYOUTS)
            or evidence_contract_version in {27, 28, 29, 30, 31, 32, 33, 51, 52, 60, 61, 62, 63, 68} and target['provider'] != 'ettoday'
            or (target['provider'] == INEWS_PROVIDER) != (evidence_contract_version in {24, 25, 26, 38, 39})
            or (target['provider'] == 'upmedia') != (evidence_contract_version in {20, 21, 22, 23})
            or (target['provider'] == 'stockfeel') != (evidence_contract_version in {14, 15})
            or (target['provider'] == 'taiwannews') != (evidence_contract_version in {16, 17, 18})
            or evidence_contract_version in BASTILLE_RESIDENT_EVIDENCE_VERSIONS and target['provider'] != 'bastillepost'):
        raise ValueError('reviewed_candidate_evidence_contract_required')
    basis = [CONTRACT_VERSION, target['provider'], target['id'], candidate]
    if evidence_contract_version == 67:
        from downloader.news_fulltext_moneyweekly_excerpt import QUESTION as MONEYWEEKLY_EXCERPT_QUESTION
        return hashlib.sha256(_bytes([67, target['provider'], target['id'], MONEYWEEKLY_EXCERPT_QUESTION])).hexdigest()
    if evidence_contract_version == 66:
        from downloader.news_fulltext_moneyweekly_metadata import QUESTION as MONEYWEEKLY_METADATA_QUESTION
        return hashlib.sha256(_bytes([66, target['provider'], target['id'], MONEYWEEKLY_METADATA_QUESTION])).hexdigest()
    if evidence_contract_version == 65:
        from downloader.news_fulltext_moneyweekly import BODY_GEOMETRY_QUESTION
        return hashlib.sha256(_bytes([65, target['provider'], target['id'], BODY_GEOMETRY_QUESTION])).hexdigest()
    if evidence_contract_version == 64:
        from downloader.news_fulltext_moneyweekly import ROUTE_METADATA_QUESTION
        return hashlib.sha256(_bytes([64, target['provider'], target['id'], ROUTE_METADATA_QUESTION])).hexdigest()
    if evidence_contract_version == 68:
        basis = [68, target['provider'], target['id'], ETTODAY_CONFLICT_ORIGINAL_QUESTION]
    elif evidence_contract_version == 63:
        basis = [63, target['provider'], target['id'], ETTODAY_ATTRIBUTION_PREDICATES_QUESTION]
    elif evidence_contract_version == 62:
        from downloader.news_fulltext_ettoday import ETTODAY_PRELUDE_EXCERPT_QUESTION
        basis = [62, target['provider'], target['id'], ETTODAY_PRELUDE_EXCERPT_QUESTION]
    elif evidence_contract_version == 61:
        from downloader.news_fulltext_ettoday import ETTODAY_CREDIT_DECLARATION_QUESTION
        basis = [61, target['provider'], target['id'], ETTODAY_CREDIT_DECLARATION_QUESTION]
    elif evidence_contract_version == 60:
        from downloader.news_fulltext_candidate_originals import ETTODAY_CAPTION_ORIGINAL_QUESTION
        basis = [60, target['provider'], target['id'], ETTODAY_CAPTION_ORIGINAL_QUESTION]
    elif evidence_contract_version == 59:
        from downloader.news_fulltext_managertoday import MANAGER_MULTIPLE_ORIGINAL_QUESTION
        basis = [59,target['provider'],target['id'],MANAGER_MULTIPLE_ORIGINAL_QUESTION]
    elif evidence_contract_version == 58:
        from downloader.news_fulltext_managertoday import MANAGER_BODY_ORIGINAL_QUESTION
        basis = [58,target['provider'],target['id'],MANAGER_BODY_ORIGINAL_QUESTION]
    elif evidence_contract_version == 57:
        from downloader.news_fulltext_knews import KNEWS_HEADER_SHAPE_QUESTION
        basis = [57, target['provider'], target['id'], KNEWS_HEADER_SHAPE_QUESTION]
    elif evidence_contract_version == 56:
        from downloader.news_fulltext_knews import KNEWS_FAILURE_ORIGINAL_QUESTION
        basis = [56, target['provider'], target['id'], KNEWS_FAILURE_ORIGINAL_QUESTION]
    elif evidence_contract_version == 55:
        from downloader.news_fulltext_contentplatform import CONTENTPLATFORM_FAILURE_SHAPE_QUESTION
        basis = [55,target['provider'],target['id'],CONTENTPLATFORM_FAILURE_SHAPE_QUESTION]
    elif evidence_contract_version == 48:
        from downloader.news_fulltext_gnn import GNN_METADATA_QUESTION
        basis = [48, target['provider'], target['id'], GNN_METADATA_QUESTION]
    elif evidence_contract_version == 49:
        from downloader.news_fulltext_gnn import GNN_BOUNDARY_ORIGINAL_QUESTION
        basis = [49, target['provider'], target['id'], GNN_BOUNDARY_ORIGINAL_QUESTION]
    elif evidence_contract_version == 50:
        from downloader.news_fulltext_gnn import GNN_ROUTE_QUESTION
        basis = [50, target['provider'], target['id'], GNN_ROUTE_QUESTION]
    elif evidence_contract_version == 51:
        from downloader.news_fulltext_ettoday import ETTODAY_SCHEMA_RELATIONSHIP_QUESTION
        basis = [51, target['provider'], target['id'], ETTODAY_SCHEMA_RELATIONSHIP_QUESTION]
    elif evidence_contract_version == 52:
        from downloader.news_fulltext_candidate_originals import ETTODAY_DISAGREEMENT_ORIGINAL_QUESTION
        basis = [52, target['provider'], target['id'], ETTODAY_DISAGREEMENT_ORIGINAL_QUESTION]
    if target['provider'] == 'cmnews' and evidence_contract_version == 1:
        basis = [1, target['provider'], target['id'], 'cmnews_current_identity_boundary_v1']
    if evidence_contract_version in {46,47}:
        from downloader.news_fulltext_managertoday import MANAGER_METADATA_QUESTION, MANAGER_PLAIN_CREDIT_QUESTION
        basis = [evidence_contract_version, target['provider'], target['id'],
            MANAGER_METADATA_QUESTION if evidence_contract_version==46 else MANAGER_PLAIN_CREDIT_QUESTION]
    elif evidence_contract_version == 2:
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
    elif evidence_contract_version == 54:
        from downloader.news_fulltext_publishers import CMNEWS_CANONICAL_ATTRIBUTE_QUESTION
        basis = [54, target['provider'], target['id'], CMNEWS_CANONICAL_ATTRIBUTE_QUESTION]
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
    elif evidence_contract_version == 38:
        basis = [38, target['provider'], target['id'], INEWS_BODY_ORIGINAL_QUESTION]
    elif evidence_contract_version == 39:
        basis = [39, target['provider'], target['id'], INEWS_SHORT_CREDIT_ORIGINAL_QUESTION]
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
    elif evidence_contract_version == 34:
        from downloader.news_fulltext_gvm import GVM_BOUNDARY_QUESTION
        basis = [34, target['provider'], target['id'], GVM_BOUNDARY_QUESTION]
    elif evidence_contract_version == 35:
        from downloader.news_fulltext_gvm import GVM_EDITOR_QUESTION
        basis = [35, target['provider'], target['id'], GVM_EDITOR_QUESTION]
    elif evidence_contract_version == 36:
        from downloader.news_fulltext_gvm import GVM_REMAINING_QUESTION
        basis = [36, target['provider'], target['id'], GVM_REMAINING_QUESTION]
    elif evidence_contract_version == 37:
        from downloader.news_fulltext_gvm import GVM_V4_FAILURE_QUESTION
        basis = [37, target['provider'], target['id'], GVM_V4_FAILURE_QUESTION]
    elif evidence_contract_version == 40:
        # This original is reusable by future parsers. Neither parser hashes,
        # changed failure counters nor another reason buys another source GET.
        basis = [40, target['provider'], target['id'], GVM_ORIGINAL_QUESTION]
    elif evidence_contract_version == 41:
        from downloader.news_fulltext_gvm import GVM_HEADER_QUESTION
        basis = [41, target['provider'], target['id'], GVM_HEADER_QUESTION]
    elif evidence_contract_version == 42:
        from downloader.news_fulltext_gvm import GVM_MULTIPLE_CREDIT_QUESTION
        basis = [42, target['provider'], target['id'], GVM_MULTIPLE_CREDIT_QUESTION]
    elif evidence_contract_version in CONTENTPLATFORM_EVIDENCE_VERSIONS:
        basis = [evidence_contract_version, target['provider'], target['id'],
            CONTENTPLATFORM_EVIDENCE_QUESTIONS[evidence_contract_version]]
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


def _prior_contentplatform_root_refusal(db, source, url):
    """The nested-root question follows one closed, unretained header failure.

    Bind the exact original head, request, source and response outcome. A
    successful retained original, transport failure or uncertain reservation
    never authorizes another request. Recheck at reservation, not just submit.
    """
    from downloader.news_fulltext_contentplatform import (
        CONTENTPLATFORM_ORIGINAL_LAYOUTS, CONTENTPLATFORM_ORIGINAL_QUESTION,
        CONTENTPLATFORM_V2_ORIGINAL_BINDING, contentplatform_original_source)
    from downloader.news_fulltext_repairs import _unique
    error = 'completed_contentplatform_unretained_header_refusal_required'
    try:
        if not contentplatform_original_source(source):
            raise ValueError(error)
        target = _head(db.execute('SELECT * FROM articles WHERE url=?', (url,)).fetchone())
        key = _key(target, source.data_fingerprint, evidence_contract_version=43)
        if not db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='native_candidate_jobs'").fetchone():
            raise ValueError(error)
        prior = db.execute('SELECT * FROM native_candidate_jobs WHERE observation_key=?', (key,)).fetchone()
        if (not prior or prior['state'] != 'observed' or any(
                not isinstance(prior[k], str) or not 0 < len(prior[k]) <= 131072
                for k in ('request_json', 'result_json'))):
            raise ValueError(error)
        request, result = (json.loads(prior[k], object_pairs_hook=_unique) for k in ('request_json', 'result_json'))
        old_source = request['source']
        location = result['private_original_scope_failure_location']
        if (hashlib.sha256(_bytes(request)).hexdigest() != prior['request_id']
                or request['target'] != target or request['observation_key'] != key
                or request['candidate_layout'] not in CONTENTPLATFORM_ORIGINAL_LAYOUTS
                or request['evidence_contract_version'] != 43 or result['evidence_contract_version'] != 43
                or set(old_source) != SOURCE_FIELDS
                or old_source['authorization_fingerprint'] != source.authorization_fingerprint
                or (old_source['data_fingerprint'], old_source['authorization_fingerprint']) not in {
                    CONTENTPLATFORM_V2_ORIGINAL_BINDING, (source.data_fingerprint, source.authorization_fingerprint)}
                or request['candidate_fingerprint'] != old_source['data_fingerprint']
                or result['question'] != CONTENTPLATFORM_ORIGINAL_QUESTION
                or result['provider'] != source.provider or result['requested_url'] != url or result['final_url'] != url
                or type(result['status']) is not int or result['status'] != 200
                or result['private_original_scope_error'] != 'article_identity_not_verified'
                or 'private_original' in result or 'private_original_permit' in result
                or any(result[k] is not False for k in ('raw_or_names_or_body_retained', 'accepted_as_article',
                    'body_completeness_verified', 'identity_or_alias_accepted'))
                or not location['sites']
                or location['sites'][-1].get('module') != 'news_fulltext_contentplatform.py'
                or location['sites'][-1].get('function') != '_native_header'
                or not (_clock(target['last_checked_at_utc']) <= _clock(prior['created_at_utc'])
                    <= _clock(prior['reserved_at_utc']) <= _clock(prior['completed_at_utc']) <= time.time())):
            raise ValueError(error)
    except (KeyError, TypeError, ValueError, RecursionError, AttributeError):
        raise ValueError(error) from None


def _prior_contentplatform_image_refusal(db, source, url):
    """Only the closed div/image refusal authorizes this distinct relation."""
    from downloader.news_fulltext_contentplatform import CONTENTPLATFORM_ROOT_QUESTION
    from downloader.news_fulltext_repairs import _unique
    _prior_contentplatform_root_refusal(db, source, url)
    error = 'completed_contentplatform_leading_image_refusal_required'
    try:
        target = _head(db.execute('SELECT * FROM articles WHERE url=?', (url,)).fetchone())
        key = _key(target, source.data_fingerprint, evidence_contract_version=44)
        prior = db.execute('SELECT * FROM native_candidate_jobs WHERE observation_key=?', (key,)).fetchone()
        if (not prior or prior['state'] != 'observed' or any(
                not isinstance(prior[k], str) or not 0 < len(prior[k]) <= 131072
                for k in ('request_json', 'result_json'))):
            raise ValueError(error)
        request, result = (json.loads(prior[k], object_pairs_hook=_unique) for k in ('request_json', 'result_json'))
        shape = result['native_root_shape']
        if (hashlib.sha256(_bytes(request)).hexdigest() != prior['request_id']
                or request['target'] != target or request['observation_key'] != key
                or request['source'] != _source_contract(db, source, url)[1]
                or request['candidate_layout'] != source.embedded_article_layout
                or request['candidate_fingerprint'] != source.data_fingerprint
                or request['evidence_contract_version'] != 44 or result['evidence_contract_version'] != 44
                or result['question'] != CONTENTPLATFORM_ROOT_QUESTION
                or result['provider'] != source.provider or result['requested_url'] != url or result['final_url'] != url
                or type(result['status']) is not int or result['status'] != 200
                or result['private_original_scope_error'] != 'private_contentplatform_nested_credit_not_verified'
                or 'private_original' in result or 'private_original_permit' in result
                or any(result[k] is not False for k in ('raw_or_names_or_body_retained', 'accepted_as_article',
                    'body_completeness_verified', 'identity_or_alias_accepted'))
                or shape['contract_version'] != 1 or shape['native_identity_author_clock_verified'] is not True
                or shape['direct_tag_counts'] != {'div': 1}
                or shape['first_written_candidate_path'] != ['div', 'img']
                or shape['first_candidate_is_nested_paragraph'] is not False
                or shape['first_credit_matches_declared_author'] is not False
                or not (_clock(target['last_checked_at_utc']) <= _clock(prior['created_at_utc'])
                    <= _clock(prior['reserved_at_utc']) <= _clock(prior['completed_at_utc']) <= time.time())):
            raise ValueError(error)
    except (KeyError, TypeError, ValueError, RecursionError, AttributeError):
        raise ValueError(error) from None


def _prior_contentplatform_prefix_refusal(db, source, url):
    """A closed whole-paragraph refusal leaves a distinct prefix question.

    Require the same failed head and source at submit and claim. Any retained
    original or unfinished predecessor forbids buying another source request.
    """
    from downloader.news_fulltext_contentplatform import CONTENTPLATFORM_ROOT_QUESTION
    from downloader.news_fulltext_repairs import _unique
    _prior_contentplatform_root_refusal(db, source, url)
    error = 'completed_contentplatform_unretained_whole_credit_refusal_required'
    try:
        target = _head(db.execute('SELECT * FROM articles WHERE url=?', (url,)).fetchone())
        key = _key(target, source.data_fingerprint, evidence_contract_version=44)
        prior = db.execute('SELECT * FROM native_candidate_jobs WHERE observation_key=?', (key,)).fetchone()
        if (not prior or prior['state'] != 'observed' or any(
                not isinstance(prior[k], str) or not 0 < len(prior[k]) <= 131072
                for k in ('request_json', 'result_json'))):
            raise ValueError(error)
        request, result = (json.loads(prior[k], object_pairs_hook=_unique) for k in ('request_json', 'result_json'))
        shape = result['native_root_shape']
        if (hashlib.sha256(_bytes(request)).hexdigest() != prior['request_id']
                or request['target'] != target or request['observation_key'] != key
                or request['source'] != _source_contract(db, source, url)[1]
                or request['candidate_layout'] != source.embedded_article_layout
                or request['candidate_fingerprint'] != source.data_fingerprint
                or request['evidence_contract_version'] != 44 or result['evidence_contract_version'] != 44
                or result['question'] != CONTENTPLATFORM_ROOT_QUESTION
                or result['provider'] != source.provider or result['requested_url'] != url or result['final_url'] != url
                or type(result['status']) is not int or result['status'] != 200
                or result['private_original_scope_error'] != 'private_contentplatform_nested_credit_not_verified'
                or 'private_original' in result or 'private_original_permit' in result
                or any(result[k] is not False for k in ('raw_or_names_or_body_retained', 'accepted_as_article',
                    'body_completeness_verified', 'identity_or_alias_accepted'))
                or shape['contract_version'] != 1 or shape['native_identity_author_clock_verified'] is not True
                or shape['first_credit_matches_declared_author'] is not False
                or shape['body_geometry_accepted'] is not False or shape['article_accepted'] is not False
                or shape['source_text_exported'] is not False
                or not (_clock(target['last_checked_at_utc']) <= _clock(prior['created_at_utc'])
                    <= _clock(prior['reserved_at_utc']) <= _clock(prior['completed_at_utc']) <= time.time())):
            raise ValueError(error)
        for row in db.execute('SELECT state,request_json,result_json FROM native_candidate_jobs WHERE provider=?',
                (source.provider,)):
            other = json.loads(row['request_json'], object_pairs_hook=_unique)
            if other['target']['id'] != target['id'] or other.get('evidence_contract_version') == 53:
                continue
            outcome = json.loads(row['result_json'], object_pairs_hook=_unique) if row['result_json'] else {}
            if row['state'] not in {'observed', 'observed_error', 'skipped'} or outcome.get('private_original'):
                raise ValueError(error)
    except (KeyError, TypeError, ValueError, RecursionError, AttributeError):
        raise ValueError(error) from None


def _prior_gvm_header_refusal(db, source, url):
    """A new header question requires a closed, same-head V8 refusal.

    The original-retention question stays closed. Only its explicit missing
    header evidence can authorize the distinct finite relation observation;
    a changed parser, reason, failed transport or unknown outcome cannot.
    Rechecked on both submission and reservation by the existing owner.
    """
    from downloader.news_fulltext_gvm import GVM_V8_ORIGINAL_BINDING, GVM_WRITTEN_FORMAT_LAYOUT, gvm_static_editor_source
    from downloader.news_fulltext_repairs import _unique

    error = 'completed_gvm_missing_header_original_refusal_required'
    try:
        if (not gvm_static_editor_source(source)
                or (source.parser_repair_prior_capture_fingerprint, source.authorization_fingerprint)
                    != GVM_V8_ORIGINAL_BINDING
                or not db.execute("SELECT 1 FROM sqlite_master WHERE name='native_candidate_jobs' AND type='table'").fetchone()):
            raise ValueError(error)
        target = _head(db.execute('SELECT * FROM articles WHERE url=?', (url,)).fetchone())
        key = _key(target, GVM_V8_ORIGINAL_BINDING[0], evidence_contract_version=40)
        prior = db.execute('SELECT * FROM native_candidate_jobs WHERE observation_key=?', (key,)).fetchone()
        if (not prior or prior['state'] != 'observed' or any(
                not isinstance(prior[k], str) or not 0 < len(prior[k]) <= 131072
                for k in ('request_json', 'result_json'))):
            raise ValueError(error)
        request, result = (json.loads(prior[k], object_pairs_hook=_unique) for k in ('request_json', 'result_json'))
        if not isinstance(request, dict) or not isinstance(result, dict):
            raise ValueError(error)
        old_source = request.get('source')
        context, location = result.get('gvm_received_failure_context'), result.get('failure_location')
        if (hashlib.sha256(_bytes(request)).hexdigest() != prior['request_id']
                or request.get('target') != target or request.get('observation_key') != key
                or not isinstance(old_source, dict) or set(old_source) != SOURCE_FIELDS
                or old_source.get('data_fingerprint') != GVM_V8_ORIGINAL_BINDING[0]
                or old_source.get('authorization_fingerprint') != GVM_V8_ORIGINAL_BINDING[1]
                or request.get('candidate_layout') != GVM_WRITTEN_FORMAT_LAYOUT
                or request.get('evidence_contract_version') != 40
                or request.get('candidate_fingerprint') != GVM_V8_ORIGINAL_BINDING[0]
                or result.get('evidence_contract_version') != 40 or result.get('question') != GVM_ORIGINAL_QUESTION
                or result.get('provider') != 'gvm' or result.get('requested_url') != url
                or result.get('final_url') != url or result.get('redirect_count') != 0 or result.get('status') != 200
                or result.get('current_native_error') != 'article_identity_not_verified'
                or result.get('private_original_scope_error') != 'private_gvm_original_scope_not_verified'
                or 'private_original' in result or 'private_original_permit' in result
                or any(result.get(k) is not False for k in ('raw_or_names_or_body_retained', 'accepted_as_article',
                    'body_completeness_verified', 'identity_or_alias_accepted'))
                or not isinstance(context, dict) or context.get('native_parser_version') != 8
                or context.get('current_native_error') != 'article_identity_not_verified'
                or context.get('prior_header_identity_access_credit_clocks_passed') is not False
                or any(context.get(k) is not True for k in ('native_frame_observed', 'native_head_found',
                    'native_body_found', 'native_unrestricted'))
                or not isinstance(location, dict) or not isinstance(location.get('sites'), list)
                or not any(isinstance(site, dict) and site.get('module') == 'news_fulltext_gvm.py'
                    and site.get('function') in {'gvm_public_native_article', '_magazine_issue'}
                    for site in location['sites'])
                # The periodical template omits the online access attribute;
                # reaching _magazine_issue proves the earlier public checks ran.
                or context.get('native_accessible') is not True and not (
                    context.get('native_accessible') is False and any(isinstance(site, dict)
                        and site.get('module') == 'news_fulltext_gvm.py' and site.get('function') == '_magazine_issue'
                        for site in location['sites']))
                or not (_clock(target['last_checked_at_utc']) <= _clock(prior['created_at_utc'])
                    <= _clock(prior['reserved_at_utc']) <= _clock(prior['completed_at_utc']) <= time.time())):
            raise ValueError(error)
    except (KeyError, TypeError, ValueError, RecursionError):
        raise ValueError(error) from None


def _prior_gvm_multiple_credit_refusal(db,source,url):
    """A closed two-credit V9 refusal, never a replay of its single-credit question."""
    from downloader.news_fulltext_gvm import GVM_V9_ORIGINAL_BINDING, GVM_HEADER_QUESTION, GVM_STATIC_EDITOR_LAYOUT, gvm_presentation_source
    from downloader.news_fulltext_repairs import _unique
    error='completed_gvm_two_credit_header_refusal_required'
    try:
        if (not gvm_presentation_source(source)
                or (source.parser_repair_prior_capture_fingerprint,source.authorization_fingerprint)!=GVM_V9_ORIGINAL_BINDING
                or not db.execute("SELECT 1 FROM sqlite_master WHERE name='native_candidate_jobs' AND type='table'").fetchone()):
            raise ValueError(error)
        target=_head(db.execute('SELECT * FROM articles WHERE url=?',(url,)).fetchone())
        key=_key(target,GVM_V9_ORIGINAL_BINDING[0],evidence_contract_version=41)
        prior=db.execute('SELECT * FROM native_candidate_jobs WHERE observation_key=?',(key,)).fetchone()
        if (not prior or prior['state']!='observed' or any(not isinstance(prior[k],str)
                or not 0<len(prior[k])<=131072 for k in ('request_json','result_json'))):raise ValueError(error)
        request,result=(json.loads(prior[k],object_pairs_hook=_unique) for k in ('request_json','result_json'))
        old=request.get('source');relations=result.get('gvm_header_relations')
        if (hashlib.sha256(_bytes(request)).hexdigest()!=prior['request_id'] or request.get('target')!=target
                or request.get('observation_key')!=key or not isinstance(old,dict) or set(old)!=SOURCE_FIELDS
                or (old.get('data_fingerprint'),old.get('authorization_fingerprint'))!=GVM_V9_ORIGINAL_BINDING
                or request.get('candidate_layout')!=GVM_STATIC_EDITOR_LAYOUT or request.get('evidence_contract_version')!=41
                or request.get('candidate_fingerprint')!=GVM_V9_ORIGINAL_BINDING[0]
                or result.get('evidence_contract_version')!=41 or result.get('question')!=GVM_HEADER_QUESTION
                or result.get('provider')!='gvm' or result.get('requested_url')!=url or result.get('final_url')!=url
                or result.get('status')!=200 or result.get('redirect_count')!=0
                or result.get('current_native_error')!='article_identity_not_verified'
                or result.get('private_original_scope_error')!='private_gvm_header_original_scope_not_verified'
                or 'private_original' in result or 'private_original_permit' in result
                or any(result.get(k) is not False for k in ('raw_or_names_or_body_retained','accepted_as_article',
                    'body_completeness_verified','identity_or_alias_accepted'))
                or not isinstance(relations,dict) or relations.get('native_parser_version')!=9
                or any(relations.get(k)!=2 for k in ('native_credit_anchor_count','prior_credit_selector_count','native_credit_node_count','schema_author_count'))
                or any(relations.get(k) is not True for k in ('literal_native_identity_agrees','native_public_access',
                    'schema_publisher_and_literal_URL_agree','schema_public_access','native_publication_clocks_agree'))
                or relations.get('external_media_credit_marker') is not False or relations.get('explicit_reprint_marker') is not False
                or relations.get('private_header_profile_schema_clocks_verified') is not False
                or not (_clock(target['last_checked_at_utc'])<=_clock(prior['created_at_utc'])
                    <=_clock(prior['reserved_at_utc'])<=_clock(prior['completed_at_utc'])<=time.time())):
            raise ValueError(error)
    except (AttributeError,KeyError,TypeError,ValueError,RecursionError):raise ValueError(error) from None


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


def _prior_knews_missing_header(db, source, url):
    """Only an unanswered header structure after the exact closed raw refusal."""
    from downloader.news_fulltext_knews import (
        KNEWS_FAILURE_ORIGINAL_QUESTION, KNEWS_GRID_NATIVE_LAYOUT, KNEWS_TABLE_NATIVE_LAYOUT, KNEWS_TABLE_HTTP_BINDING)
    error = 'completed_knews_unretained_native_header_refusal_required'
    try:
        row = db.execute('SELECT * FROM articles WHERE url=?', (url,)).fetchone()
        target = _head(row)
        prior = db.execute('SELECT * FROM native_candidate_jobs WHERE observation_key=?',
            (_key(target, source.data_fingerprint, evidence_contract_version=56),)).fetchone()
        if prior is None:
            raise ValueError(error)
        request, result = json.loads(prior['request_json']), json.loads(prior['result_json'])
        old_source = request['source']
        sites = result['private_original_scope_failure_location']['sites']
        if (source.embedded_article_layout != KNEWS_GRID_NATIVE_LAYOUT
                or target != request['target'] or target['state'] != 'article_identity_not_verified'
                or prior['state'] != 'observed' or request['evidence_contract_version'] != 56
                or set(old_source) != SOURCE_FIELDS
                or old_source['authorization_fingerprint'] != source.authorization_fingerprint
                or ((old_source['data_fingerprint'], old_source['authorization_fingerprint'])
                    != (source.data_fingerprint, source.authorization_fingerprint)
                    and ((old_source['data_fingerprint'], old_source['authorization_fingerprint']) != KNEWS_TABLE_HTTP_BINDING[:2]
                        or (source.http_capacity_source_fingerprint, source.http_capacity_authorization_fingerprint)
                            != KNEWS_TABLE_HTTP_BINDING[2:]))
                or request['candidate_fingerprint'] != old_source['data_fingerprint']
                or request['candidate_layout'] not in {KNEWS_TABLE_NATIVE_LAYOUT, KNEWS_GRID_NATIVE_LAYOUT}
                or result['question'] != KNEWS_FAILURE_ORIGINAL_QUESTION
                or result['provider'] != source.provider or result['requested_url'] != url or result['final_url'] != url
                or type(result['status']) is not int or result['status'] != 200
                or result['private_original_scope_error'] != 'article_identity_not_verified'
                or 'private_original' in result or 'private_original_permit' in result
                or any(result[k] is not False for k in ('raw_or_names_or_body_retained', 'accepted_as_article',
                    'body_completeness_verified', 'identity_or_alias_accepted'))
                or len(sites) < 2 or sites[-2].get('function') not in {'_native_frame', '_native_credit'}
                or sites[-1].get('function') != '_one'
                or sites[-2].get('module') != 'news_fulltext_knews.py'
                or sites[-1].get('module') != 'news_fulltext_public_originals.py'
                or not (_clock(target['last_checked_at_utc']) <= _clock(prior['created_at_utc'])
                    <= _clock(prior['reserved_at_utc']) <= _clock(prior['completed_at_utc']) <= time.time())):
            raise ValueError(error)
    except (KeyError, TypeError, ValueError, RecursionError, AttributeError):
        raise ValueError(error) from None


def _prior_ettoday_empty_credit_declarations(db, source, url):
    """An excerpt is distinct from, and cannot rearm, the closed credit question."""
    from downloader.news_fulltext_ettoday import ETTODAY_CREDIT_DECLARATION_QUESTION
    error = 'closed_empty_ettoday_credit_declarations_required'
    try:
        target = _head(db.execute('SELECT * FROM articles WHERE url=?', (url,)).fetchone())
        prior = db.execute('SELECT state,request_json,result_json FROM native_candidate_jobs WHERE observation_key=?',
            (_key(target, source.data_fingerprint, evidence_contract_version=61),)).fetchone()
        request, result = json.loads(prior['request_json']), json.loads(prior['result_json'])
        header = result['ettoday_native_header']
        if (prior['state'] != 'observed' or request['target'] != target
                or request['evidence_contract_version'] != 61
                or request['source'] != _source_contract(db, source, url)[1]
                or request['candidate_fingerprint'] != source.data_fingerprint
                or result['question'] != ETTODAY_CREDIT_DECLARATION_QUESTION
                or result['status'] != 200 or result['requested_url'] != url or result['final_url'] != url
                or result['accepted_as_article'] is not False or result['raw_or_body_retained'] is not False
                or result['raw_or_names_or_body_retained'] is not False
                or header['native_credit_declarations'] != []
                or not all(header['native_identity_relations'].values())
                or 'private_original' in result or 'private_original_permit' in result):
            raise ValueError(error)
    except (KeyError, TypeError, ValueError, RecursionError, sqlite3.OperationalError):
        raise ValueError(error) from None


def _prior_ettoday_short_prelude(db, source, url):
    """Closed excerpt remains reusable after a later failed article attempt.

    The current request/claim still freezes its canonical current head. Only the
    two monotonically advancing attempt fields may differ from question 62;
    this neither reopens that question nor permits a changed body or source.
    """
    error = 'closed_ettoday_short_prelude_required'
    try:
        target = _head(db.execute('SELECT * FROM articles WHERE url=?', (url,)).fetchone())
        prior = db.execute('SELECT state,request_json,result_json FROM native_candidate_jobs WHERE observation_key=?',
            (_key(target, source.data_fingerprint, evidence_contract_version=62),)).fetchone()
        request, result = json.loads(prior['request_json']), json.loads(prior['result_json'])
        excerpt, old = result['ettoday_unclassified_prelude'], request['target']
        same = lambda head: {k:v for k,v in head.items() if k not in {'attempts','last_checked_at_utc'}}
        if (prior['state'] != 'observed' or same(old) != same(target)
                or type(old['attempts']) is not int or target['attempts'] < old['attempts']
                or _clock(target['last_checked_at_utc']) < _clock(old['last_checked_at_utc'])
                or request['evidence_contract_version'] != 62
                or request['source'] != _source_contract(db, source, url)[1]
                or request['candidate_fingerprint'] != source.data_fingerprint
                or result['question'] != ETTODAY_PRELUDE_EXCERPT_QUESTION
                or result['requested_url'] != url or result['final_url'] != url or result['status'] != 200
                or result['accepted_as_article'] is not False or result['diagnostic_excerpt_retained'] is not True
                or result['raw_response_retained'] is not False or result['full_body_retained'] is not False
                or excerpt['question'] != ETTODAY_PRELUDE_EXCERPT_QUESTION or excerpt['paragraph_index'] != 2
                or not isinstance(excerpt['text'], str) or not 1 <= len(excerpt['text']) <= 160
                or excerpt['text_sha256'] != hashlib.sha256(excerpt['text'].encode()).hexdigest()
                or excerpt['author_or_supplier_accepted'] is not False
                or 'private_original' in result or 'private_original_permit' in result):
            raise ValueError(error)
    except (KeyError, TypeError, ValueError, AttributeError, RecursionError, sqlite3.OperationalError):
        raise ValueError(error) from None


def _prior_ettoday_creator_conflict(db, source, url):
    """Reuse the closed attribution observation without replaying its question."""
    error = 'closed_ettoday_creator_conflict_required'
    try:
        target = _head(db.execute('SELECT * FROM articles WHERE url=?', (url,)).fetchone())
        prior = db.execute('SELECT state,request_json,result_json FROM native_candidate_jobs WHERE observation_key=?',
            (_key(target, source.data_fingerprint, evidence_contract_version=63),)).fetchone()
        request, result = json.loads(prior['request_json']), json.loads(prior['result_json'])
        old, facts = request['target'], result['ettoday_attribution_predicates']
        same = lambda head: {k: v for k, v in head.items() if k not in {'attempts', 'last_checked_at_utc'}}
        creator = facts['creator_values']
        article_id = urlsplit(url).path.rsplit('/', 1)[-1]
        if (prior['state'] != 'observed' or same(old) != same(target)
                or type(old['attempts']) is not int or target['attempts'] < old['attempts']
                or _clock(target['last_checked_at_utc']) < _clock(old['last_checked_at_utc'])
                or request['evidence_contract_version'] != 63
                or request['source'] != _source_contract(db, source, url)[1]
                or request['candidate_fingerprint'] != source.data_fingerprint
                or not _ettoday_attribution_predicates_result(result, url)
                or not facts['literal_role_name'] or not facts['creator_is_list']
                or facts['creator_values_withheld'] or facts['creator_count'] != 2
                or not facts['creator_identifier_valid'] or facts['creator_matches_literal_role_name']
                or not isinstance(creator, list) or len(creator) != 2
                or creator[1] != article_id or not creator[0].startswith(article_id + '-')
                or len(creator[0]) <= len(article_id) + 1
                or not facts['author_is_exact_publisher_organization']
                or not facts['publisher_is_expected_organization'] or facts['authors_truncated']
                or facts['author_person_matches_literal_role_name']
                or any(item['parse_succeeded'] or item['error_code'] != 'article_attribution_not_verified'
                    or item['attribution_failure_stage'] != 'bare_reporter_creator_mismatch'
                    for item in facts['parser_outcomes'])):
            raise ValueError(error)
        keys = ('prelude_text_sha256', 'literal_role_name', 'creator_values',
            'published_at_raw', 'modified_at_raw', 'native_authors', 'native_publisher')
        return {key: facts[key] for key in keys}
    except (KeyError, TypeError, ValueError, AttributeError, RecursionError, sqlite3.OperationalError):
        raise ValueError(error) from None


def _question_source(db, registry, url, evidence):
    if evidence == 67:
        from downloader.news_fulltext_moneyweekly_excerpt import require_prior_excerpt
        source = require_prior_excerpt(db, registry.require(url), url)
        return _source_contract(db, source, url)
    if evidence == 66:
        from downloader.news_fulltext_moneyweekly_metadata import require_prior_template
        source = require_prior_template(db, registry.require(url), url)
        return _source_contract(db, source, url)
    if evidence == 65:
        from downloader.news_fulltext_moneyweekly import moneyweekly_body_geometry_route, moneyweekly_body_geometry_source
        source = registry.require(url)
        if not moneyweekly_body_geometry_source(source) or not moneyweekly_body_geometry_route(url):
            raise ValueError('literal_known_moneyweekly_body_geometry_route_required')
        return _source_contract(db, source, url)
    if evidence == 64:
        from downloader.news_fulltext_moneyweekly import (
            moneyweekly_literal_navigation_route, moneyweekly_literal_unhandled_route,
            moneyweekly_route_source, require_prior_moneyweekly_navigation,
        )
        source = registry.require(url)
        if moneyweekly_literal_navigation_route(url):
            require_prior_moneyweekly_navigation(db, source, url)
        elif not moneyweekly_route_source(source) or not moneyweekly_literal_unhandled_route(url):
            raise ValueError('literal_known_moneyweekly_unhandled_route_required')
        return _source_contract(db, source, url)
    if evidence != 7:
        source, identity = _source(db, registry, url)
        if evidence in {61, 62, 63, 68}:
            from downloader.news_fulltext_ettoday import ettoday_resident_source
            if (not ettoday_resident_source(source, layout='ettoday_visible_article_v41')
                    or not ettoday_literal_diagnostic_url(url)
                    or urlsplit(url).netloc != 'finance.ettoday.net'):
                raise ValueError('literal_current_ettoday_finance_credit_route_required')
            if evidence == 62:
                _prior_ettoday_empty_credit_declarations(db, source, url)
            elif evidence == 63:
                _prior_ettoday_short_prelude(db, source, url)
            elif evidence == 68:
                _prior_ettoday_creator_conflict(db, source, url)
        if evidence == 60:
            from downloader.news_fulltext_ettoday import ettoday_resident_source
            if (not ettoday_resident_source(source, layout='ettoday_visible_article_v40')
                    or not ettoday_literal_diagnostic_url(url)
                    or urlsplit(url).netloc != 'finance.ettoday.net'):
                raise ValueError('literal_current_ettoday_finance_caption_route_required')
        if evidence == 57:
            _prior_knews_missing_header(db, source, url)
        if evidence == 50:
            from downloader.news_fulltext_gnn import GNN_LEGACY_LAYOUT, gnn_literal_route, gnn_metadata_source
            # A previous destination authorization failure is NOT permission
            # to follow it. Recheck the current ORIGINAL route only.
            if (not gnn_metadata_source(source) or source.embedded_article_layout != GNN_LEGACY_LAYOUT
                    or not gnn_literal_route(url)):
                raise ValueError('literal_registered_GNN_route_required')
        if evidence == 47:
            _prior_manager_no_profile_header(db, source, url)
        if evidence == 58:
            _prior_manager_embedded_document(db,source,url)
        if evidence == 59:
            _prior_manager_multiple_roots(db,source,url)
        if evidence==41:
            _prior_gvm_header_refusal(db,source,url)
        if evidence==42:
            _prior_gvm_multiple_credit_refusal(db,source,url)
        if evidence == 44:
            _prior_contentplatform_root_refusal(db, source, url)
        if evidence == 45:
            _prior_contentplatform_image_refusal(db, source, url)
        if evidence == 53:
            _prior_contentplatform_prefix_refusal(db, source, url)
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
        if evidence == 54:
            _unanswered_cmnews_canonical_attribute(db, source, identity, url)
        if evidence == 21:
            _prior_upmedia_missing_header(db, source, identity, url)
        if evidence == 22:
            _prior_upmedia_editor_geometry(db, source, identity, url)
        if evidence == 23:
            _prior_upmedia_remaining_context(db, source, identity, url)
        if evidence in {25, 26}:
            _prior_inews_printed_credit(db, source, identity, url, header_credit=evidence == 26)
        if evidence == 39:
            _prior_inews_short_credit(db, source, identity, url)
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
        if evidence == 51:
            _prior_ettoday_schema_relationship_refusal(db, source, identity, url)
        if evidence == 52:
            _prior_ettoday_person_disagreement(db, source, identity, url)
        return source, identity
    from downloader.news_fulltext_pocket_scope import non_school_route
    source = registry.require(url)
    if source.provider != 'pocket' or non_school_route(url) is None:
        raise ValueError('literal_non_school_pocket_route_required')
    return _source_contract(db, source, url)


def _prior_manager_multiple_roots(db,source,url):
    """A distinct original question after the closed two-header/body shape."""
    from downloader.news_fulltext_managertoday import MANAGER_LAYOUTS,MANAGER_METADATA_QUESTION,manager_metadata_source
    error='closed_manager_two_root_shape_required'
    if not manager_metadata_source(source) or source.embedded_article_layout!='manager_public_written_article_v5':
        raise ValueError(error)
    if not db.execute("SELECT 1 FROM sqlite_master WHERE name='native_candidate_jobs' AND type='table'").fetchone():
        raise ValueError(error)
    rows=db.execute("SELECT state,request_json,result_json FROM native_candidate_jobs WHERE provider='managertoday' "
        "AND json_extract(request_json,'$.evidence_contract_version')=46 "
        "AND json_extract(request_json,'$.target.url')=?",(url,)).fetchall()
    try:
        if len(rows)!=1 or rows[0]['state']!='observed':raise ValueError
        request,result=json.loads(rows[0]['request_json']),json.loads(rows[0]['result_json'])
        native=result['manager_native_header'];shapes=native['body_structure_without_prose']
        if (request['candidate_layout'] not in MANAGER_LAYOUTS or result['question']!=MANAGER_METADATA_QUESTION
                or result['requested_url']!=url or result['final_url']!=url or result['status']!=200
                or result['raw_or_body_retained'] is not False or result['accepted_as_article'] is not False
                or native['header_count']!=2 or native['body_container_count']!=2 or native['main_container_count']!=2
                or len(shapes)!=2 or any(s['node_limit_reached'] for s in shapes)):
            raise ValueError
    except (ValueError,KeyError,TypeError):raise ValueError(error) from None


def _prior_manager_embedded_document(db,source,url):
    """A new raw-document question after a closed, bounded shape observation."""
    from downloader.news_fulltext_managertoday import MANAGER_LAYOUTS, MANAGER_METADATA_QUESTION, manager_metadata_source
    error = 'closed_manager_failed_embedded_document_shape_required'
    if not manager_metadata_source(source) or source.embedded_article_layout!='manager_public_written_article_v4':
        raise ValueError(error)
    if not db.execute("SELECT 1 FROM sqlite_master WHERE name='native_candidate_jobs' AND type='table'").fetchone():
        raise ValueError(error)
    rows = db.execute("SELECT state,request_json,result_json FROM native_candidate_jobs WHERE provider='managertoday' "
        "AND json_extract(request_json,'$.evidence_contract_version')=46 "
        "AND json_extract(request_json,'$.target.url')=?",(url,)).fetchall()
    try:
        if len(rows)!=1 or rows[0]['state']!='observed':
            raise ValueError
        request,result = json.loads(rows[0]['request_json']),json.loads(rows[0]['result_json'])
        native = result['manager_native_header']
        shape, = native['body_structure_without_prose']
        if (result['question']!=MANAGER_METADATA_QUESTION or result['requested_url']!=url
                or result['final_url']!=url or result['status']!=200
                or result['raw_or_body_retained'] is not False or result['accepted_as_article'] is not False
                # Old bounded shapes are evidence, never an access grant.
                # The current source and newly received raw header are gated
                # independently; no refetch of the closed old question.
                or request['candidate_layout'] not in MANAGER_LAYOUTS
                or native['header_count']!=1 or native['body_container_count']!=1 or native['main_container_count']!=1
                or shape['node_limit_reached'] or not any(t['has_table_cells'] is False
                    and t['x_ref']=='src' and t['parent_tag']=='div' and t['parent_x_data']=='iframe'
                    and set(t['parent_classes'])=={'widget','w-full','iframe'} for t in shape['templates'])):
            raise ValueError
    except (ValueError,KeyError,TypeError):
        raise ValueError(error) from None


def _prior_manager_no_profile_header(db, source, url):
    """Only a closed, bounded v46 no-profile result buys this NEW question."""
    from downloader.news_fulltext_managertoday import MANAGER_METADATA_QUESTION, manager_metadata_source
    if not manager_metadata_source(source):
        raise ValueError('current_manager_source_required')
    if not db.execute("SELECT 1 FROM sqlite_master WHERE name='native_candidate_jobs' AND type='table'").fetchone():
        raise ValueError('closed_manager_no_profile_header_required')
    rows=db.execute("SELECT state,result_json FROM native_candidate_jobs WHERE provider='managertoday' "
        "AND json_extract(request_json,'$.evidence_contract_version')=46 "
        "AND json_extract(request_json,'$.target.url')=?",(url,)).fetchall()
    try:
        if len(rows)!=1 or rows[0]['state']!='observed':
            raise ValueError
        result=json.loads(rows[0]['result_json'])
        native=result['manager_native_header']
        if (result['question']!=MANAGER_METADATA_QUESTION or result['requested_url']!=url
                or result['final_url']!=url or result['status']!=200
                or result['raw_or_body_retained'] is not False
                or native['header_count']!=1 or len(native['headers'])!=1
                or native['headers'][0]['credits']!=[]):
            raise ValueError
    except (ValueError,KeyError,TypeError):
        raise ValueError('closed_manager_no_profile_header_required') from None


def _prior_ettoday_inline_credit(db, source, identity, url):
    """A new inline-prefix question, only after the closed plain-line refusal."""
    _prior_ettoday_leading_credit(db,source,identity,url)
    try:
        target = _head(db.execute('SELECT * FROM articles WHERE url=?',(url,)).fetchone())
        prior = db.execute('SELECT * FROM native_candidate_jobs WHERE observation_key=?',
            (_key(target,source.data_fingerprint,evidence_contract_version=28),)).fetchone()
        if not prior or prior['state'] != 'observed_error':
            raise _ETtodayInlinePredecessorError('state')
        if any(not isinstance(prior[k],str) or not 0 < len(prior[k]) <= 131072
               for k in ('request_json','result_json')):
            raise _ETtodayInlinePredecessorError('payload')
        request, result = json.loads(prior['request_json']), json.loads(prior['result_json'])
        if request['target'] != target or request['source'] != identity or request['evidence_contract_version'] != 28:
            raise _ETtodayInlinePredecessorError('identity')
        if (result['error'] != 'private_ettoday_credit_original_scope_not_verified'
                or result['raw_or_names_or_body_retained'] is not False or result['accepted_as_article'] is not False):
            raise _ETtodayInlinePredecessorError('result')
        if not any(site['module'] == 'news_fulltext_candidate_originals.py'
                   and site['function'] == 'private_ettoday_credit_original_permit'
                   for site in result['failure_location']['sites']):
            raise _ETtodayInlinePredecessorError('site')
        if hashlib.sha256(prior['request_json'].encode()).hexdigest() != prior['request_id']:
            raise _ETtodayInlinePredecessorError('hash')
        if not (_clock(prior['created_at_utc']) <= _clock(prior['reserved_at_utc'])
                <= _clock(prior['completed_at_utc']) <= time.time()):
            raise _ETtodayInlinePredecessorError('clock')
    except _ETtodayInlinePredecessorError:
        raise
    except (KeyError,IndexError,TypeError,ValueError,RecursionError):
        raise _ETtodayInlinePredecessorError('malformed') from None


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


def _prior_ettoday_schema_relationship_refusal(db, source, identity, url):
    """A closed combined predicate refusal permits one distinct metadata question.

    The old original-retention question remains closed. No raw response,
    names, body, changed parser or clock can rearm this fixed question.
    """
    _prior_ettoday_canonical_credit(db, source, identity, url)
    error = 'completed_ettoday_combined_schema_relationship_refusal_required'
    try:
        target = _head(db.execute('SELECT * FROM articles WHERE url=?', (url,)).fetchone())
        prior = db.execute('SELECT * FROM native_candidate_jobs WHERE observation_key=?',
            (_key(target, source.data_fingerprint, evidence_contract_version=33),)).fetchone()
        if (not prior or prior['state'] != 'observed'
                or any(not isinstance(prior[k], str) or not 0 < len(prior[k]) <= 131072
                       for k in ('request_json', 'result_json'))):
            raise ValueError(error)
        request, result = json.loads(prior['request_json']), json.loads(prior['result_json'])
        trace = result['visible_credit_scope']
        if (request['source'] != identity or request['target'] != target
                or request['evidence_contract_version'] != 33 or result['evidence_contract_version'] != 33
                or result['provider'] != 'ettoday' or result['question'] != ETTODAY_CANONICAL_ORIGINAL_QUESTION
                or result['private_original_scope_error'] != 'article_rights_or_scope_excluded'
                or trace['verification_stage'] != 'same_article_author'
                or trace['separator_found'] is not True or trace['following_visible_text'] is not True
                or not any(site['module'] == 'news_fulltext_ettoday.py' and site['function'] == '_schemas'
                           for site in result['failure_location']['sites'])
                or result['status'] != 200 or result['requested_url'] != url or result['final_url'] != url
                or result['redirect_count'] != 0 or result['raw_or_names_or_body_retained'] is not False
                or result['accepted_as_article'] is not False or 'private_original' in result
                or 'private_original_permit' in result
                or hashlib.sha256(prior['request_json'].encode()).hexdigest() != prior['request_id']
                or not (_clock(prior['created_at_utc']) <= _clock(prior['reserved_at_utc'])
                        <= _clock(prior['completed_at_utc']) <= time.time())):
            raise ValueError(error)
    except (KeyError, IndexError, TypeError, ValueError, RecursionError):
        raise ValueError(error) from None


def _prior_ettoday_person_disagreement(db, source, identity, url):
    """Only the completed identity/clock/publisher-positive Person disagreement.

    The new private-original question does not retry the metadata question or
    resolve the conflict. Reserve once and independently recheck actual bytes.
    """
    from downloader.news_fulltext_ettoday import ETTODAY_SCHEMA_RELATIONSHIP_QUESTION
    _prior_ettoday_schema_relationship_refusal(db, source, identity, url)
    error = 'completed_ettoday_literal_person_disagreement_required'
    expected = {'identity_and_source_clocks_verified': True, 'publisher_is_object': True,
        'publisher_is_organization': True, 'publisher_name_matches': True,
        'schema_author_count': 1, 'schema_author_is_exact_publisher': False,
        'schema_author_matches_last_of_two': False, 'schema_authors_all_person': True,
        'schema_authors_match_all_visible_reporters': False, 'schema_authors_match_ordered_subset': False,
        'visible_reporter_count': 1}
    try:
        target = _head(db.execute('SELECT * FROM articles WHERE url=?', (url,)).fetchone())
        prior = db.execute('SELECT * FROM native_candidate_jobs WHERE observation_key=?',
            (_key(target, source.data_fingerprint, evidence_contract_version=51),)).fetchone()
        if (not prior or prior['state'] != 'observed' or any(
                not isinstance(prior[k], str) or not 0 < len(prior[k]) <= 131072 for k in ('request_json', 'result_json'))):
            raise ValueError(error)
        request, result = json.loads(prior['request_json']), json.loads(prior['result_json'])
        if (request['target'] != target or request['source'] != identity or request['evidence_contract_version'] != 51
                or result['evidence_contract_version'] != 51 or result['question'] != ETTODAY_SCHEMA_RELATIONSHIP_QUESTION
                or result['provider'] != 'ettoday' or result['schema_relationships'] != expected
                or any(type(v) is not type(expected[k]) for k, v in result['schema_relationships'].items())
                or result['scope_error'] != 'article_rights_or_scope_excluded'
                or result['status'] != 200 or result['requested_url'] != url or result['final_url'] != url
                or result['redirect_count'] != 0 or result['accepted_as_article'] is not False
                or result['raw_or_names_or_body_retained'] is not False or 'private_original' in result
                or 'private_original_permit' in result
                or hashlib.sha256(prior['request_json'].encode()).hexdigest() != prior['request_id']
                or not (_clock(prior['created_at_utc']) <= _clock(prior['reserved_at_utc'])
                        <= _clock(prior['completed_at_utc']) <= time.time())):
            raise ValueError(error)
    except (KeyError, IndexError, TypeError, ValueError, RecursionError):
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


def _prior_inews_short_credit(db, source, identity, url):
    """Only the closed v38 short editor/printed-credit gap, not a new v38 GET."""
    error = 'completed_inews_short_editor_or_printed_credit_proof_required'
    try:
        target = _head(db.execute('SELECT * FROM articles WHERE url=?', (url,)).fetchone())
        key = _key(target, source.data_fingerprint, evidence_contract_version=38)
        prior = db.execute('SELECT * FROM native_candidate_jobs WHERE observation_key=?', (key,)).fetchone()
        if not prior or prior['state'] != 'observed':
            raise ValueError(error)
        request, facts = json.loads(prior['request_json']), json.loads(prior['result_json'])
        boundary = facts['inews_native_boundary']
        credits, editors = boundary['credits'], boundary['editors']
        body = editors[0]['bodies'][0]
        credit = credits[0]
        printed = (credit['prefix_candidate']=='empty' and credit['element']['characters']==0
            and not credit['links'])
        named_editor = (credit['prefix_candidate']=='編輯' and len(credit['links'])==1
            and credit['links'][0]['native_author'] is True and not credit['links'][0]['hidden']
            and 2 <= credit['links'][0]['characters'] <= 20 and not credit['links'][0]['child_tags'])
        paragraphs = [r for r in body['children'] if r['tag']=='p']
        if (source.provider!=INEWS_PROVIDER or request['source']!=identity or request['target']!=target
                or request['evidence_contract_version']!=38 or facts['evidence_contract_version']!=38
                or facts['question']!=INEWS_BODY_ORIGINAL_QUESTION or facts['provider']!=INEWS_PROVIDER
                or facts['requested_url']!=url or facts['final_url']!=url or facts['redirect_count']!=0
                or facts['status']!=200 or facts['accepted_as_article'] is not False
                or facts['raw_or_names_or_body_retained'] is not False
                or facts['native_short_body_scope_verified'] is not False
                or boundary['context_truncated'] or boundary['global_shape_limit_reached']
                or len(editors)!=1 or len(credits)!=1 or editors[0]['body_count']!=1
                or editors[0]['bodies_truncated'] or body['children_truncated']
                or credit['element']['hidden'] or credit['links_truncated'] or not (printed or named_editor)
                or not 0 < sum(p['characters'] for p in paragraphs) < 100
                or any(r['tag'] not in {'p','figure'} for r in body['children'])
                or hashlib.sha256(prior['request_json'].encode()).hexdigest()!=prior['request_id']
                or not (_clock(prior['created_at_utc']) <= _clock(prior['reserved_at_utc'])
                    <= _clock(prior['completed_at_utc']) <= time.time())):
            raise ValueError(error)
    except (sqlite3.OperationalError, KeyError, IndexError, TypeError, ValueError, RecursionError):
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
    if _cmnews_canonical_question_already_answered(db, source, target):
        raise ValueError('cmnews_literal_canonical_already_recorded')


def _unanswered_cmnews_canonical_attribute(db, source, identity, url):
    """Only a missing literal declaration, never a replay of a retained one.

    Early failed heads may have no native observation. A closed ASCII-only
    observation permits this distinct URI/IRI question only when it attests
    the exact native head but explicitly did not retain its sole canonical.
    The shared pending/uncertain-outcome barrier still applies to both cases.
    """
    from downloader.news_fulltext_publishers import CMNEWS_CANONICAL_RELATION_QUESTION
    from downloader.news_fulltext_repairs import _unique
    row = db.execute('SELECT * FROM articles WHERE url=?', (url,)).fetchone()
    target = _head(row)
    if _cmnews_canonical_question_already_answered(db, source, target):
        raise ValueError('cmnews_literal_canonical_already_recorded')
    if not db.execute("SELECT 1 FROM sqlite_master WHERE name='native_candidate_jobs' AND type='table'").fetchone():
        return
    prior = db.execute('SELECT * FROM native_candidate_jobs WHERE observation_key=?',
        (_key(target, source.data_fingerprint, evidence_contract_version=19),)).fetchone()
    if prior is None:
        return
    error = 'prior_unretained_cmnews_canonical_required'
    try:
        if (prior['state'] != 'observed' or any(not isinstance(prior[k], str)
                or not 0 < len(prior[k]) <= 131072 for k in ('request_json', 'result_json'))):
            raise ValueError(error)
        request, facts = (json.loads(prior[k], object_pairs_hook=_unique) for k in ('request_json', 'result_json'))
        if not isinstance(request, dict) or not isinstance(facts, dict):
            raise ValueError(error)
        native, relation = facts['native_identity'], facts['canonical_relation']
        if not isinstance(native, dict) or not isinstance(relation, dict):
            raise ValueError(error)
        if (hashlib.sha256(_bytes(request)).hexdigest() != prior['request_id']
                or request['source'] != identity or request['target'] != target
                or request['evidence_contract_version'] != 19
                or request['candidate_fingerprint'] != source.data_fingerprint
                or request['candidate_layout'] != 'cmnews_visible_article_v1'
                or facts['provider'] != 'cmnews' or facts['question'] != CMNEWS_CANONICAL_RELATION_QUESTION
                or facts['evidence_contract_version'] != 19 or facts['status'] != 200
                or any(facts.get(k) is not False for k in ('accepted_as_article',
                    'raw_or_names_or_body_retained', 'body_completeness_verified', 'identity_or_alias_accepted'))
                or any(type(native.get(k)) is not int or native[k] != expected for k, expected in {
                    'canonical_count': 1, 'matching_canonical_count': 0, 'matching_native_scope_count': 1,
                    'native_title_count': 1, 'nonempty_native_title_count': 1,
                    'native_body_container_count': 1}.items())
                or native.get('native_slug_matches') is not True
                or native.get('native_UUID_container_matches') is not True
                or native.get('ambiguous_native_scope') is not False
                or relation.get('canonical_count') != 1
                or relation.get('canonical_locators') != [{'literal_locator_retained': False}]
                or relation.get('canonical_list_truncated') is not False
                or relation.get('article_or_alias_accepted') is not False
                or not (_clock(target['last_checked_at_utc']) <= _clock(prior['created_at_utc'])
                    <= _clock(prior['reserved_at_utc']) <= _clock(prior['completed_at_utc']) <= time.time())):
            raise ValueError(error)
    except (KeyError, IndexError, TypeError, ValueError, RecursionError):
        raise ValueError(error) from None


_CMNEWS_V7_LITERAL_ONLY_DATA_FINGERPRINT = '58422bd43af9a159846a87cd92f0e1bd078d50afac315eb35dc523842c997405'
_CMNEWS_V8_LITERAL_ONLY_BINDING = (
    '6a8e7c6375cc75d16dc29a8640e2172a964b77b76de59ff4b5d119403e2af986',
    'fcf9934ef3ead98ebc10cd7569648dee7f83756f8dd8b716bfa1b0509a2c9d6c',
)
# The successor differs only in successful CMNews schema-clock provenance;
# no URL, source scope, auth, HTTP behavior or identity failure changes.
_CMNEWS_CLOCK_BASIS_V1_LITERAL_ONLY_BINDING = (
    'a2d9e9762fae902d64969a54494dbf2c46ac44446d10499385b9e478bfeb16f2',
    'fcf9934ef3ead98ebc10cd7569648dee7f83756f8dd8b716bfa1b0509a2c9d6c',
)
# Only these exact prior failed checks and retained metadata were reviewed.
# A later head, modified receipt, or another v7 record needs its own proof.
# See docs/news_all_source_repair_2026-10-09.md, 10/29 CMNews review.
_CMNEWS_V7_REVIEWED_LITERAL_CHECKS = {
    '8135c8d7e507b870e747f991a9de529afd01b32c7d22b0f55a4bdb8911a3d9aa': (
        '2026-10-20T02:44:27.851622+00:00',
        'ed628d6c62d0d7050e7558dd9477081b2c0d174f44ca17f4f9c8c3e1ff1a02cf'),
    'e0d4076b6c06df4e8e30423a29e036f9010128d41ef50df90b39d86270a07d93': (
        '2026-10-20T02:44:28.210776+00:00',
        'b85a638ad20fb996ff4edac432f11ea72e2948c1484bdf1de6a66d04b19f1b98'),
}


def _cmnews_canonical_question_already_answered(db, source, target):
    """Old literal evidence may stop a duplicate question, never admit a URL.

    The v7 literal receipts remain individually pinned; the exact v8 to
    clock-basis successor preserves identity failure semantics. Neither path
    grants article admission. Other source/auth changes stay incompatible.
    """
    reviewed = _CMNEWS_V7_REVIEWED_LITERAL_CHECKS.get(target['id'])
    binding = (source.data_fingerprint, source.authorization_fingerprint)
    compatible = (binding in {_CMNEWS_V8_LITERAL_ONLY_BINDING, _CMNEWS_CLOCK_BASIS_V1_LITERAL_ONLY_BINDING}
                  and source.provider == 'cmnews' and source.allowed
                  and source.embedded_article_layout == 'cmnews_visible_article_v1'
                  and reviewed is not None and target['last_checked_at_utc'] == reviewed[0])
    equivalent_failure = (_CMNEWS_V8_LITERAL_ONLY_BINDING[0]
                          if binding == _CMNEWS_CLOCK_BASIS_V1_LITERAL_ONLY_BINDING
                          and source.provider == 'cmnews' and source.allowed
                          and source.embedded_article_layout == 'cmnews_visible_article_v1' else None)
    return _read_recorded_cmnews_canonical(db, source, target,
        legacy=_CMNEWS_V7_LITERAL_ONLY_DATA_FINGERPRINT if compatible else None,
        legacy_sha256=reviewed[1] if compatible else None,
        equivalent_failure=equivalent_failure) is not None


def recorded_cmnews_canonical(db, source, target):
    """Reuse exact prior failure metadata before asking a new HTTP question.

    Only the failed check's UTC day and preceding day can contain its receipt
    (HTTP receipt and queue finish may cross midnight). No full-store walk,
    body decode, article acceptance, alias join or archive mutation is needed.
    The display URL must hash to the original literal, excluding truncation
    and query/fragment redaction. Source identity and exact request also bind.
    """
    return _read_recorded_cmnews_canonical(db, source, target)


def _read_recorded_cmnews_canonical(db, source, target, *, legacy=None, legacy_sha256=None,
                                    equivalent_failure=None):
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
                version = facts.get('contract_version')
                data_fingerprint = facts.get('data_fingerprint')
                if (type(version) is not int or version not in IDENTITY_METADATA_READ_VERSIONS
                        or facts.get('provider') != 'cmnews'
                        or (data_fingerprint != source.data_fingerprint
                            and (legacy is None or data_fingerprint != legacy)
                            and (equivalent_failure is None or data_fingerprint != equivalent_failure))
                        or facts.get('authorization_fingerprint') != source.authorization_fingerprint
                        or facts.get('request_url') != target['url'] or facts.get('final_url') != target['url']
                        or facts.get('request_url_sha256') != target['id'] or facts.get('final_url_sha256') != target['id']
                        or facts.get('error_code') != 'article_identity_not_verified' or facts.get('http_status') != 200
                        or facts.get('diagnostic_only') is not True or facts.get('raw_response_retained') is not False
                        or facts.get('accepted_as_article') is not False):
                    continue
                observed = _clock(facts.get('observed_at_utc'))
                if (observed > checked.timestamp()
                        or (legacy is not None and data_fingerprint == legacy
                            and (checked.timestamp() - observed > 60 or legacy_sha256 is None))):
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
                metadata_sha256 = hashlib.sha256(payload).hexdigest()
                if legacy is not None and data_fingerprint == legacy and metadata_sha256 != legacy_sha256:
                    continue
                return {'canonical_url': literal, 'metadata_path': str(path),
                    'metadata_sha256': metadata_sha256,
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
    if candidate.provider == 'moneyweekly':
        from downloader.news_fulltext_moneyweekly_metadata import LAYOUT
        from downloader.news_fulltext_moneyweekly_excerpt import LAYOUT as EXCERPT_LAYOUT
        from downloader.news_fulltext_moneyweekly import BODY_GEOMETRY_LAYOUT, ROUTE_METADATA_LAYOUT
        if {64: ROUTE_METADATA_LAYOUT, 65: BODY_GEOMETRY_LAYOUT, 66: LAYOUT, 67: EXCERPT_LAYOUT}.get(evidence_contract_version) != layout:
            raise ValueError('reviewed_candidate_evidence_contract_required')
    if not candidate_evidence_allowed(registry.sources[candidate.provider], candidate, evidence_contract_version):
        raise ValueError('reviewed_candidate_evidence_contract_required')
    if evidence_contract_version in {24, 25, 26, 38, 39} and not inews_literal_article_url(url):
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
                    7, 12, 13, 14, 15, 18, 20, 21, 22, 23, 24, 25, 26, 27, 34, 35, 36, 37, 38, 39, 40, 41, 42, 48, 49, 50, 55, 56, 57, 60, 61, 62, 63, 64, 65, 66, 67, 68, *CONTENTPLATFORM_EVIDENCE_VERSIONS, *BASTILLE_RESIDENT_EVIDENCE_VERSIONS}) and _prior_unfinished_observation(db, target):
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
    from downloader.news_fulltext_gvm import GVM_CANDIDATE_LAYOUTS
    from downloader.news_fulltext_contentplatform import CONTENTPLATFORM_ORIGINAL_LAYOUTS, contentplatform_article_id
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
                or request['candidate_layout'] in {'moneyweekly_known_route_metadata_v1', 'moneyweekly_known_body_geometry_v1', 'moneyweekly_unresolved_template_metadata_v1', 'moneyweekly_short_template_and_query_declaration_v1'}
                or request['candidate_layout'] in CONTENTPLATFORM_ORIGINAL_LAYOUTS
                or request['candidate_layout'] == 'contentplatform_native_article_v7'
                or request['candidate_layout'] in GVM_CANDIDATE_LAYOUTS.values()
                or request['candidate_layout'] in INEWS_LAYOUT_VERSIONS
                or request['candidate_layout'] in {'upmedia_public_modern_article_v1', 'upmedia_public_modern_article_v2', 'upmedia_public_modern_article_v3', 'upmedia_public_modern_article_v4'}
                or request['candidate_layout'] in {'taiwannews_completed_public_original_v2', 'taiwannews_completed_public_original_v3'}
                or request['candidate_layout'] in {'manager_public_written_article_v1', 'manager_public_written_article_v2', 'manager_public_written_article_v3', 'manager_public_written_article_v4', 'manager_public_written_article_v5'}
                or request['candidate_layout'] in {'gnn_native_reporter_article_v1', 'gnn_native_credited_article_v2', 'gnn_native_written_article_v3', 'gnn_native_legacy_article_v4'}
                or request['candidate_layout'] in {'knews_public_native_article_v7', 'knews_public_native_article_v8', 'knews_public_native_article_v9', 'knews_public_native_article_v10'}
                or request['candidate_layout'] == GLOBALVOICES_ORIGINAL_LAYOUT
                or request['candidate_layout'] in {'stockfeel_visible_original_v1', 'stockfeel_visible_original_v2', 'stockfeel_visible_original_v3', 'stockfeel_visible_original_v4', 'stockfeel_visible_original_v5', 'stockfeel_visible_original_v6', 'stockfeel_visible_original_v7'}
                or BASTILLE_LAYOUT_VERSIONS.get(request['candidate_layout'], 0) >= 2 or re.fullmatch(
                    r'(?:cmnews_visible_article_v1|pocket_visible_article_v[0-9]{1,2}|videoland_visible_article_v4|watchmedia_visible_article_v3|ettoday_visible_article_v(?:39|40|41)|max_visible_post_v[0-9]{1,2})',
                    request['candidate_layout']))
            or not isinstance(request['candidate_fingerprint'], str) or not _SAFE_HASH.fullmatch(request['candidate_fingerprint'])
            or not isinstance(request['observation_key'], str)):
        raise ValueError('invalid_native_candidate_request')
    if request['candidate_layout'] in {'moneyweekly_known_route_metadata_v1', 'moneyweekly_known_body_geometry_v1', 'moneyweekly_unresolved_template_metadata_v1', 'moneyweekly_short_template_and_query_declaration_v1'}:
        from downloader.news_fulltext_moneyweekly_excerpt import LAYOUT as EXCERPT_LAYOUT
        from downloader.news_fulltext_moneyweekly_metadata import LAYOUT
        from downloader.news_fulltext_moneyweekly import BODY_GEOMETRY_LAYOUT, ROUTE_METADATA_LAYOUT
        if {64: ROUTE_METADATA_LAYOUT, 65: BODY_GEOMETRY_LAYOUT, 66: LAYOUT, 67: EXCERPT_LAYOUT}.get(evidence) != request['candidate_layout']:
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
                target['provider'] != 'cmnews' or evidence not in {1, 19, 54})
            or evidence in {19, 54} and (target['provider'] != 'cmnews' or request['candidate_layout'] != 'cmnews_visible_article_v1')
            or (request['candidate_layout'] in {'knews_public_native_article_v7', 'knews_public_native_article_v8', 'knews_public_native_article_v9', 'knews_public_native_article_v10'}) != (evidence in {56, 57})
            or evidence in {56, 57} and (target['provider'] != 'knews'
                or not re.fullmatch(r'https://www[.]knews[.]com[.]tw/news/[0-9A-F]{32}', target['url']))
            or (request['candidate_layout'] in {'gnn_native_reporter_article_v1', 'gnn_native_credited_article_v2', 'gnn_native_written_article_v3', 'gnn_native_legacy_article_v4'}) != (evidence in {48,49,50})
            or evidence in {48,49,50} and target['provider'] != 'gnn'
            or evidence == 50 and request['candidate_layout'] != 'gnn_native_legacy_article_v4'
            or (request['candidate_layout'] in CONTENTPLATFORM_ORIGINAL_LAYOUTS) != (evidence in CONTENTPLATFORM_EVIDENCE_VERSIONS)
            or evidence in CONTENTPLATFORM_EVIDENCE_VERSIONS and (target['provider'] != 'contentplatform' or contentplatform_article_id(target['url']) is None)
            or evidence == 55 and (target['provider'] != 'contentplatform' or request['candidate_layout'] != 'contentplatform_native_article_v7'
                or contentplatform_article_id(target['url']) is None)
            or (request['candidate_layout'] in GVM_CANDIDATE_LAYOUTS.values() or evidence in GVM_CANDIDATE_LAYOUTS)
                and (target['provider']!='gvm' or GVM_CANDIDATE_LAYOUTS.get(evidence)!=request['candidate_layout'])
            or evidence in {27, 28, 29, 30, 31, 32, 33, 51, 52} and (target['provider'] != 'ettoday' or request['candidate_layout'] != 'ettoday_visible_article_v39')
            or request['candidate_layout'] == 'ettoday_visible_article_v39' and evidence not in {1, 27, 28, 29, 30, 31, 32, 33, 51, 52}
            or (request['candidate_layout'] == 'ettoday_visible_article_v40') != (evidence == 60)
            or (request['candidate_layout'] == 'ettoday_visible_article_v41') != (evidence in {61, 62, 63, 68})
            or evidence in {61, 62, 63, 68} and (target['provider'] != 'ettoday'
                or not ettoday_literal_diagnostic_url(target['url'])
                or urlsplit(target['url']).netloc != 'finance.ettoday.net')
            or evidence == 60 and (target['provider'] != 'ettoday'
                or not ettoday_literal_diagnostic_url(target['url'])
                or urlsplit(target['url']).netloc != 'finance.ettoday.net')
            or (request['candidate_layout'] in INEWS_LAYOUT_VERSIONS) != (evidence in {24, 25, 26, 38, 39})
            or evidence in {24, 25, 26, 38, 39} and (target['provider'] != INEWS_PROVIDER or not inews_literal_article_url(target['url']))
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
    if evidence in {27, 28, 29, 30, 31, 32, 33, 51, 52} and not ettoday_literal_diagnostic_url(target['url'], allow_house=True):
        # Host review is also checked against the current registry at claim.
        raise ValueError('literal_current_ettoday_route_required')
    if evidence == 50:
        from downloader.news_fulltext_gnn import gnn_literal_route
        if not gnn_literal_route(target['url']):
            raise ValueError('literal_registered_GNN_route_required')
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
        queue.db.execute('CREATE TABLE IF NOT EXISTS native_candidate_wait('
            'request_id TEXT PRIMARY KEY,not_before_epoch REAL NOT NULL,last_gate_code TEXT NOT NULL,'
            'last_deferred_at_utc TEXT NOT NULL,deferral_count INTEGER NOT NULL)')
        queue.db.execute('CREATE TABLE IF NOT EXISTS native_candidate_schedule('
            'key TEXT PRIMARY KEY,value TEXT NOT NULL)')
        # A dispatch reservation is the unknown-outcome barrier, not permission
        # to send again. Committed results are archived below without any GET.
        queue.db.execute("UPDATE native_candidate_jobs SET state='uncertain_outcome',completed_at_utc=? "
            "WHERE state='reserved' AND owner_id<>?", (utc_now(), queue.owner))
    queue.native_candidate_schema_ready = True


def _candidate_file_signature(path):
    value = path.lstat()
    return (value.st_dev, value.st_ino, value.st_mode, value.st_size,
            value.st_mtime_ns, value.st_ctime_ns)


def _local_retry_now():
    return time.monotonic()


def _local_retry_signature(path):
    try:
        return _candidate_file_signature(path)
    except OSError:
        return None


def _defer_candidate_local_io(queue, path, reason):
    """Retry one local input without delaying unrelated publisher work."""
    if reason not in {'request_io', 'quarantine', 'archive'}:
        raise ValueError('invalid_candidate_local_retry_reason')
    retries = getattr(queue, '_candidate_local_retries', None)
    if retries is None:
        retries = queue._candidate_local_retries = {}
    signature = _local_retry_signature(path)
    old = retries.get(path.name)
    attempts = old[2] + 1 if old and old[0] == signature else 1
    delay = min(_LOCAL_RETRY_MAX_SECONDS, _LOCAL_RETRY_INITIAL_SECONDS * 2 ** min(attempts - 1, 10))
    retries[path.name] = (signature, _local_retry_now() + delay, attempts, reason)


def _candidate_local_retry_state(queue, path):
    retries = getattr(queue, '_candidate_local_retries', {})
    retry = retries.get(path.name)
    if retry is None:
        return None
    if _local_retry_signature(path) != retry[0]:
        retries.pop(path.name, None)
        return None
    return 'waiting' if _local_retry_now() < retry[1] else 'due'


def _prune_candidate_local_retries(queue, paths):
    retries = getattr(queue, '_candidate_local_retries', {})
    names = {path.name for path in paths}
    for name in tuple(retries):
        if name not in names:
            del retries[name]


def candidate_local_retry_delay(queue, maximum):
    """Bring an idle owner back when retained local work is due."""
    retries = getattr(queue, '_candidate_local_retries', {})
    if not retries:
        return maximum
    remaining = min(value[1] for value in retries.values()) - _local_retry_now()
    # An overdue accessible input wakes via has_unadmitted_candidates. If its
    # mailbox is temporarily inaccessible, keep a bounded poll instead of a
    # zero-delay service loop.
    return min(maximum, remaining if remaining > 0 else PUMP_INTERVAL_SECONDS)


def _candidate_file_identity(path, previous, cache):
    """Reuse a validated identity only while the private file is unchanged."""
    before = _candidate_file_signature(path)
    cached = previous.get(path.name)
    if cached is not None and cached[0] == before:
        provider, observation_key = cached[1:]
    else:
        request = _load(path)
        provider, observation_key = request['target']['provider'], request['observation_key']
        if _candidate_file_signature(path) != before:
            raise ValueError('candidate_request_changed_during_read')
    cache[path.name] = (before, provider, observation_key)
    return provider, observation_key


def candidate_progress(queue):
    """Bounded owner-side view of diagnostic work, never article completion.

    Pending files are submissions, and registered jobs are observations; they
    are separate counts because a registered request may still have its file.
    Do not create a schema, move a file, fetch a publisher or expose requests.
    """
    # A reservation belongs only to its dispatching owner. A previous owner's
    # reservation must not appear as currently executing before recovery runs.
    states = ('ready', 'reserved', 'orphaned_reservation', 'uncertain_outcome')
    keys = ('pending_unadmitted', *states)
    zero = lambda: {key: 0 for key in keys}
    result = {'contract_version': 1, 'available': True, 'request_files': 0,
              'pending_unadmitted': 0, 'already_registered': 0, 'invalid_or_unreadable': 0,
              'jobs': {state: 0 for state in states}, 'providers': {}, 'unregistered': zero(),
              'local_retry': {'request_io': 0, 'quarantine': 0, 'archive': 0}}

    def bucket(provider):
        if (isinstance(provider, str) and re.fullmatch(r'[a-z][a-z0-9_]{0,63}', provider)
                and provider in queue.registry.sources):
            return result['providers'].setdefault(provider, zero())
        return result['unregistered']

    try:
        has_jobs = bool(queue.db.execute("SELECT 1 FROM sqlite_master WHERE type='table' "
                                         "AND name='native_candidate_jobs'").fetchone())
        if has_jobs:
            rows = queue.db.execute("SELECT provider, CASE WHEN state='reserved' AND owner_id<>? "
                "THEN 'orphaned_reservation' ELSE state END AS projected_state, count(*) "
                "FROM native_candidate_jobs WHERE state IN ('ready','reserved','uncertain_outcome') "
                "GROUP BY provider,projected_state LIMIT 4097", (queue.owner,)).fetchall()
            if len(rows) > 4096:
                raise ValueError('candidate_progress_provider_limit')
            for provider, state, count in rows:
                if state not in states or type(count) is not int or count < 0:
                    raise ValueError('candidate_progress_invalid_job_count')
                result['jobs'][state] += count
                bucket(provider)[state] += count
        directory = queue.root
        for part in ('reviewed_repairs', 'candidate_diagnostics', 'pending'):
            directory = directory / part
            try:
                info = directory.lstat()
            except FileNotFoundError:
                queue._candidate_progress_cache = {}
                return result
            if not stat.S_ISDIR(info.st_mode) or info.st_mode & 0o077:
                raise ValueError('candidate_inbox_not_private_directory')
        paths = _pending(directory)
        if len(paths) > MAX_PENDING:
            raise ValueError('candidate_inbox_exceeds_limit')
        result['request_files'] = len(paths)
        previous = getattr(queue, '_candidate_progress_cache', {})
        local_retries = getattr(queue, '_candidate_local_retries', {})
        cache = {}
        for path in paths:
            local_retry = local_retries.get(path.name)
            if (local_retry is not None and local_retry[0] == _local_retry_signature(path)
                    and local_retry[3] in result['local_retry']):
                result['local_retry'][local_retry[3]] += 1
            try:
                provider, observation_key = _candidate_file_identity(path, previous, cache)
                job = (queue.db.execute('SELECT provider FROM native_candidate_jobs WHERE observation_key=?',
                                        (observation_key,)).fetchone() if has_jobs else None)
                if job is not None:
                    if job[0] != provider:
                        raise ValueError('candidate_progress_provider_mismatch')
                    result['already_registered'] += 1
                else:
                    result['pending_unadmitted'] += 1
                    bucket(provider)['pending_unadmitted'] += 1
            except (OSError, ValueError, TypeError, KeyError, OverflowError, RecursionError, NewsError):
                result['invalid_or_unreadable'] += 1
        queue._candidate_progress_cache = cache
    except (OSError, ValueError, sqlite3.Error):
        return {'contract_version': 1, 'available': False}
    return result


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
    from downloader.news_fulltext_wake import clear_mailbox_scan_retry, defer_mailbox_scan_retry
    try:
        try:
            base.lstat()
        except FileNotFoundError:
            clear_mailbox_scan_retry(queue, 'candidate')
            return
        paths = _pending(_inbox(queue.root))[:MAX_PENDING]
        _schema(queue)
        paths = _rotation_order(queue, paths, cursor_key=_CANDIDATE_CURSOR_KEY,
                                cursor_table='native_candidate_schedule')
    except (OSError, ValueError, sqlite3.OperationalError):
        defer_mailbox_scan_retry(queue, 'candidate')
        queue.metrics.count('local_repairs', 'candidate_inbox_unavailable')
        return
    clear_mailbox_scan_retry(queue, 'candidate')
    _prune_candidate_local_retries(queue, paths)
    considered = 0
    for path in paths:
        # Filesystem discovery can exceed the soft budget. Still process one
        # bounded request, then rotate so a slow or already-ready question
        # cannot starve the others. An explicit stop always wins.
        if stop is not None and stop.is_set() or considered and time.monotonic()-started >= max_seconds:
            break
        if _candidate_local_retry_state(queue, path) == 'waiting':
            continue
        considered += 1
        try:
            request = _load(path)
            provider = request['target']['provider']
            if (provider not in queue.registry.sources or not queue.registry.sources[provider].allowed
                    or not queue.registry.sources[provider].request_enabled):
                raise ValueError('candidate_provider_no_longer_allowed')
        except (OSError, ValueError, TypeError, KeyError, OverflowError, RecursionError, NewsError) as error:
            if stop is not None and stop.is_set():
                break
            if isinstance(error, OSError) and not _unsafe_request_path(path, error):
                # An unreadable file is not evidence that its sealed payload
                # is invalid. Keep it for a bounded local retry.
                _defer_candidate_local_io(queue, path, 'request_io')
                queue.metrics.count('local_repairs', 'candidate_request_io_deferred')
                with queue.db:
                    _record_turn(queue, path, stop, cursor_key=_CANDIDATE_CURSOR_KEY,
                                 cursor_table='native_candidate_schedule')
                continue
            try:
                _quarantine(queue, path, error)
            except (OSError, ValueError):
                queue.metrics.count('local_repairs', 'candidate_quarantine_deferred')
                _defer_candidate_local_io(queue, path, 'quarantine')
            else:
                getattr(queue, '_candidate_local_retries', {}).pop(path.name, None)
            with queue.db:
                _record_turn(queue, path, stop, cursor_key=_CANDIDATE_CURSOR_KEY,
                             cursor_table='native_candidate_schedule')
            continue
        if stop is not None and stop.is_set():
            break
        with queue.db:
            queue.db.execute('INSERT OR IGNORE INTO native_candidate_jobs '
                '(request_id,observation_key,provider,state,request_json,created_at_utc,owner_id) VALUES (?,?,?,?,?,?,?)',
                (path.stem, request['observation_key'], request['target']['provider'], 'ready',
                 _bytes(request).decode(), request['created_at_utc'], queue.owner))
            _record_turn(queue, path, stop, cursor_key=_CANDIDATE_CURSOR_KEY,
                         cursor_table='native_candidate_schedule')
        job = queue.db.execute('SELECT * FROM native_candidate_jobs WHERE observation_key=?',
            (request['observation_key'],)).fetchone()
        if job['state'] in _TERMINAL:
            try:
                _archive(queue, path, job, duplicate_of=job['request_id'] if job['request_id'] != path.stem else None)
            except (OSError, ValueError):
                queue.metrics.count('local_repairs', 'candidate_archive_deferred')
                _defer_candidate_local_io(queue, path, 'archive')
            else:
                getattr(queue, '_candidate_local_retries', {}).pop(path.name, None)
        else:
            getattr(queue, '_candidate_local_retries', {}).pop(path.name, None)
    queue.native_candidate_ready_providers = {row[0] for row in queue.db.execute(
        "SELECT DISTINCT provider FROM native_candidate_jobs WHERE state='ready'")}


def has_unadmitted_candidates(queue, *, strict=False):
    """Wake an idle owner for NEW input, not for an already-paused ready job."""
    directory = queue.root/'reviewed_repairs'/'candidate_diagnostics'/'pending'
    try:
        for parent in (directory.parent.parent, directory.parent, directory):
            mode = parent.lstat().st_mode
            if not stat.S_ISDIR(mode) or mode & 0o077:
                getattr(queue, '_candidate_local_retries', {}).clear()
                return False
        paths = _pending(directory)[:MAX_PENDING]
        names = [path.stem for path in paths]
    except FileNotFoundError:
        getattr(queue, '_candidate_local_retries', {}).clear()
        return False
    except OSError:
        if strict:
            raise
        return False
    if not names:
        _prune_candidate_local_retries(queue, paths)
        return False
    if not getattr(queue, 'native_candidate_schema_ready', False):
        return True
    registered = {row[0]: (row[1], row[2], row[3]) for row in queue.db.execute(
        'SELECT request_id,provider,observation_key,state FROM native_candidate_jobs WHERE request_id IN ('
        +','.join('?' for _ in names)+')', names)}
    # Concurrent submissions can have different file hashes but the same
    # observation. They share the registered job, so they must not keep an
    # idle owner awake while that job is paused. Invalid or changed input still
    # wakes the owner to quarantine or admit it in the normal path.
    _prune_candidate_local_retries(queue, paths)
    previous = getattr(queue, '_candidate_progress_cache', {})
    cache = {path.name: previous[path.name] for path in paths if path.name in previous}
    for path in paths:
        local_retry = _candidate_local_retry_state(queue, path)
        if local_retry == 'waiting':
            continue
        if local_retry == 'due':
            return True
        try:
            provider, observation_key = _candidate_file_identity(path, previous, cache)
            row = registered.get(path.stem)
            if row is None:
                match = queue.db.execute('SELECT provider,observation_key,state FROM native_candidate_jobs '
                                         'WHERE observation_key=?', (observation_key,)).fetchone()
                row = tuple(match) if match is not None else None
            if row is None or row[0] != provider or row[1] != observation_key or row[2] in _TERMINAL:
                return True
        except (OSError, ValueError, TypeError, KeyError, OverflowError, RecursionError, NewsError):
            return True
    queue._candidate_progress_cache = cache
    return False


def claim_candidate(queue, http, config, provider):
    if (provider not in getattr(queue, 'native_candidate_ready_providers', ())
            or http.provider_not_before.get(provider, 0) > time.time()):
        return None
    for job in queue.db.execute("SELECT j.* FROM native_candidate_jobs AS j "
            'LEFT JOIN native_candidate_wait AS w ON w.request_id=j.request_id '
            "WHERE j.state='ready' AND j.provider=? AND coalesce(w.not_before_epoch,0)<=? "
            'ORDER BY j.created_at_utc LIMIT ?', (provider, time.time(), MAX_PENDING)).fetchall():
        request = json.loads(job['request_json'])
        target = request['target']
        evidence_version = request.get('evidence_contract_version', 1)
        parts = urlsplit(target['url'])
        if (parts.netloc in http.blocked_gateway_hosts(provider)
                or parts.scheme+'://'+parts.netloc in http.blocked_robots_origins(provider)):
            continue
        try:
            if evidence_version in {7, 12, 13, 14, 15, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 46, 47, 48, 49, 50, 51, 52, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63, 64, 65, 66, 67, 68, *CONTENTPLATFORM_EVIDENCE_VERSIONS, *BASTILLE_RESIDENT_EVIDENCE_VERSIONS}:
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
            if evidence_version in {27, 28, 29, 30, 31, 32, 33, 51, 52} and not ettoday_literal_diagnostic_url(target['url'],
                    allow_house='house.ettoday.net' in source.reviewed_article_subsites):
                raise ValueError('literal_current_ettoday_route_required')
            if source.provider != provider or identity != request['source']:
                raise ValueError('source_identity_changed')
            candidate = native_candidate_source(config, queue.registry, queue.registry.base, request['candidate_layout'])
            if candidate.provider != provider or candidate.data_fingerprint != request['candidate_fingerprint']:
                raise ValueError('candidate_identity_changed')
            if not candidate_evidence_allowed(source, candidate, evidence_version):
                raise ValueError('reviewed_candidate_evidence_contract_required')
            expected_conflict = (_prior_ettoday_creator_conflict(queue.db, source, target['url'])
                if evidence_version == 68 else None)
        except (ValueError, NewsError) as error:
            outcome = {'reason': error.code if isinstance(error, NewsError) else 'head_or_source_no_longer_eligible'}
            if isinstance(error, _ETtodayInlinePredecessorError):
                # Fixed labels only: do not write source text, exception values,
                # clock values or traceback locals into the private receipt.
                outcome['ettoday_inline_predecessor_guard'] = error.detail
            with queue.db:
                queue.db.execute("UPDATE native_candidate_jobs SET state='skipped',completed_at_utc=?,owner_id=?,result_json=? "
                    'WHERE request_id=?', (utc_now(), queue.owner,
                        _bytes(outcome).decode(), job['request_id']))
            continue
        # The same main-thread writer owns reservation and future completion.
        # Reserve durably before handing any HTTP operation to the executor.
        with queue.db:
            queue.db.execute("UPDATE native_candidate_jobs SET state='reserved',reserved_at_utc=?,owner_id=? "
                "WHERE request_id=? AND state='ready'", (utc_now(), queue.owner, job['request_id']))
        result = {'request_id': job['request_id'], 'provider': provider, 'url': target['url'],
            'source': source, 'candidate': candidate, 'evidence_contract_version': evidence_version}
        if evidence_version == 68:
            result.update(expected_conflict=expected_conflict, frozen_target=target, frozen_source=identity)
        return result
    return None


def candidate_next_wake_delay(queue, provider_cooldowns, request_scope_cooldowns, maximum, *,
                              provider_ids=None, dispatch_after=None):
    """Wake for bounded future candidate gates without spinning on ready jobs.

    Ready work has already had a dispatch opportunity in this cycle. Only a
    future known deadline can cause an earlier wake here; an eligible but
    predecessor-blocked job must not start an empty service-cycle loop.
    """
    if not getattr(queue, 'native_candidate_schema_ready', False):
        return maximum
    now = time.time()
    deadlines = []
    for provider in getattr(queue, 'native_candidate_ready_providers', ()):
        source = queue.registry.sources.get(provider)
        if source is None or not source.request_enabled or provider_ids is not None and provider not in provider_ids:
            continue
        # Equality on (state, provider) uses the existing ready index; bound
        # each provider separately so another publisher cannot be hidden by a
        # large provider's first rows or trigger a sort of all ready jobs.
        rows = queue.db.execute("SELECT j.request_json,w.not_before_epoch "
            'FROM native_candidate_jobs AS j LEFT JOIN native_candidate_wait AS w ON w.request_id=j.request_id '
            "WHERE j.state='ready' AND j.provider=? ORDER BY j.created_at_utc LIMIT ?",
            (provider, MAX_PENDING)).fetchall()
        for row in rows:
            try:
                url = json.loads(row['request_json'])['target']['url']
                parts = urlsplit(url)
                origin = parts.scheme + '://' + parts.netloc
                scope = request_scope_cooldowns.get(provider, {})
                due = max(row['not_before_epoch'] or 0, provider_cooldowns.get(provider, 0),
                    scope.get('hosts', {}).get(parts.netloc, 0), scope.get('origins', {}).get(origin, 0))
                cutoff = now if dispatch_after is None else dispatch_after
                if type(due) in (int, float) and math.isfinite(due) and due > cutoff:
                    deadlines.append(due)
            except (ValueError, TypeError, KeyError, AttributeError, RecursionError):
                continue  # The normal claim path handles malformed or changed work.
    return min(maximum, max(0, min(deadlines) - now)) if deadlines else maximum


def fetch_candidate(http, job):
    evidence_version = job.get('evidence_contract_version', 1)
    if (job['provider'] != job['source'].provider
            or not candidate_evidence_allowed(job['source'], job['candidate'], evidence_version)):
        raise ValueError('reviewed_candidate_evidence_contract_required')
    started = time.perf_counter()
    response = (http.get(job['url'], follow_redirects=False)
        if evidence_version in {50,55,56,57,58,59,60,61,62,63,64,65,66,67,68} else http.get(job['url']))
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
        **({'requested_url':job['url']} if evidence_version in {5, 6, 7, 12, 13, 14, 15, 16, 17, 18, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 46, 47, 48, 49, 50, 51, 52, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63, 64, 65, 66, 67, 68, *CONTENTPLATFORM_EVIDENCE_VERSIONS, *BASTILLE_RESIDENT_EVIDENCE_VERSIONS} else {}),
        **({'expected_conflict':job.get('expected_conflict')} if evidence_version == 68 else {}))
    parsed = time.perf_counter()
    permit = None
    if evidence_version in {15, 16, 17, 18, 25, 26, 28, 29, 30, 31, 32, 33, 38, 39, 40, 41, 42, 49, 52, 56, 58, 59, 60, 68, *CONTENTPLATFORM_EVIDENCE_VERSIONS}:
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


def _ettoday_attribution_predicates_result(result, url):
    """A schema-name question cannot become an arbitrary-text retention path."""
    from downloader.news_fulltext_metrics import ETTODAY_ATTRIBUTION_FAILURE_STAGES

    def short(value, bound):
        return value is None or isinstance(value, str) and 1 <= len(value) <= bound and not re.search(r'[\x00-\x1f\x7f]', value)

    def named(value):
        return (isinstance(value, dict) and set(value) == {'type','name'}
            and value['type'] in {None,'Person','Organization'} and short(value['name'], 80))

    keys = {'contract_version','evidence_contract_version','provider','question','status','requested_url','final_url',
        'redirect_count','response_bytes','response_sha256','current_source_not_candidate','accepted_as_article',
        'public_schema_declarations_retained','raw_or_names_or_body_retained','raw_or_body_retained',
        'raw_response_retained','full_body_retained','body_completeness_verified','identity_or_alias_accepted',
        'ettoday_attribution_predicates'}
    if not isinstance(result, dict) or set(result) - {'timing'} != keys:
        return False
    facts = result['ettoday_attribution_predicates']
    fact_keys = {'contract_version','question','published_at_raw','modified_at_raw','native_identity_relations',
        'prelude_text_sha256','literal_role_name','creator_values','creator_is_list','creator_count','creator_values_withheld',
        'creator_identifier_valid','creator_matches_literal_role_name','native_authors','authors_truncated',
        'author_is_exact_publisher_organization','author_person_matches_literal_role_name','publisher_is_expected_organization',
        'native_publisher','parser_outcomes','author_or_supplier_accepted','raw_response_retained','full_body_retained','source_values_rewritten'}
    if not isinstance(facts, dict) or set(facts) != fact_keys:
        return False
    try:
        if (result['contract_version'] != 1 or result['evidence_contract_version'] != 63 or result['provider'] != 'ettoday'
                or result['question'] != ETTODAY_ATTRIBUTION_PREDICATES_QUESTION
                or result['status'] != 200 or result['redirect_count'] != 0
                or result['requested_url'] != url or result['final_url'] != url
                or type(result['response_bytes']) is not int or not 0 < result['response_bytes'] <= 2 << 20
                or not isinstance(result['response_sha256'], str) or not _SAFE_HASH.fullmatch(result['response_sha256'])
                or any(result[key] is not True for key in ('current_source_not_candidate','public_schema_declarations_retained','raw_or_names_or_body_retained'))
                or any(result[key] is not False for key in ('accepted_as_article','raw_or_body_retained','raw_response_retained',
                    'full_body_retained','body_completeness_verified','identity_or_alias_accepted'))
                or facts['contract_version'] != 1 or facts['question'] != ETTODAY_ATTRIBUTION_PREDICATES_QUESTION
                or not isinstance(facts['prelude_text_sha256'], str) or not _SAFE_HASH.fullmatch(facts['prelude_text_sha256'])
                or not short(facts['literal_role_name'], 8)
                or any(facts[key] is not False for key in ('author_or_supplier_accepted','raw_response_retained','full_body_retained','source_values_rewritten'))):
            return False
        relations = facts['native_identity_relations']
        if (not isinstance(relations, dict) or set(relations) != {'url_equals_literal_request',
                'main_entity_equals_literal_request','headline_equals_single_native_heading'}
                or any(value is not True for value in relations.values())):
            return False
        _clock(facts['published_at_raw']); _clock(facts['modified_at_raw'])
        values = facts['creator_values']
        if values is not None and (not isinstance(values, list) or len(values) > 4
                or any(not isinstance(value, str) or not short(value, 160) for value in values)):
            return False
        if (facts['creator_count'] is not None and (type(facts['creator_count']) is not int or not 0 <= facts['creator_count'] <= 2 << 20)
                or not isinstance(facts['native_authors'], list) or len(facts['native_authors']) > 4
                or not all(named(value) for value in facts['native_authors']) or not named(facts['native_publisher'])
                or any(type(facts[key]) is not bool for key in ('creator_is_list','creator_values_withheld',
                    'creator_identifier_valid','creator_matches_literal_role_name','authors_truncated',
                    'author_is_exact_publisher_organization','author_person_matches_literal_role_name','publisher_is_expected_organization'))):
            return False
        outcomes = facts['parser_outcomes']
        if not isinstance(outcomes, list) or len(outcomes) != 2:
            return False
        for version, outcome in zip((41,42), outcomes):
            if (not isinstance(outcome, dict) or set(outcome) != {'version','parse_succeeded','error_code',
                    'attribution_failure_stage','failure_sites'} or outcome['version'] != version
                    or type(outcome['parse_succeeded']) is not bool
                    or outcome['parse_succeeded'] != (outcome['error_code'] is None)
                    or outcome['error_code'] is not None and (not isinstance(outcome['error_code'], str)
                        or not re.fullmatch(r'[a-z][a-z0-9_]{0,95}', outcome['error_code']))
                    or outcome['attribution_failure_stage'] not in ETTODAY_ATTRIBUTION_FAILURE_STAGES | {None}
                    or not isinstance(outcome['failure_sites'], list) or len(outcome['failure_sites']) > 8):
                return False
            for site in outcome['failure_sites']:
                if (not isinstance(site, dict) or set(site) != {'module','function','line'}
                        or not re.fullmatch(r'news_fulltext(?:_[a-z_]+)?[.]py', site['module'])
                        or not re.fullmatch(r'[_a-zA-Z][_a-zA-Z0-9]{0,120}', site['function'])
                        or type(site['line']) is not int or not 0 < site['line'] <= 1000000):
                    return False
        if 'timing' in result:
            timing = result['timing']
            if (not isinstance(timing, dict) or set(timing) != {'http_seconds','parse_seconds','scope_seconds','total_seconds'}
                    or any(type(value) not in (int,float) or not math.isfinite(value) or not 0 <= value <= 3600 for value in timing.values())):
                return False
        return True
    except (TypeError, ValueError, KeyError, AttributeError, RecursionError):
        return False


def finish_candidate(queue, job, *, result=None, error=None):
    if (isinstance(error, NewsError) and error.request_deferred is True and error.retry is True
            and error.code in REQUEST_DEFERRAL_CODES and getattr(error, 'initial_request_not_sent', False) is True
            and type(error.delay) in (int, float) and math.isfinite(error.delay) and error.delay >= 0):
        # get() proves only that this observation's initial URL was not sent;
        # prior robots requests remain counted. A crash before this transaction
        # still recovers the reservation as an unknown outcome, not a retry.
        with queue.db:
            cursor = queue.db.execute("UPDATE native_candidate_jobs SET state='ready',reserved_at_utc=NULL "
                "WHERE request_id=? AND state='reserved' AND owner_id=?", (job['request_id'], queue.owner))
            if cursor.rowcount != 1:
                raise ValueError('candidate_completion_owner_mismatch')
            queue.db.execute('INSERT INTO native_candidate_wait VALUES (?,?,?,?,1) '
                'ON CONFLICT(request_id) DO UPDATE SET '
                'not_before_epoch=max(native_candidate_wait.not_before_epoch,excluded.not_before_epoch),'
                'last_gate_code=excluded.last_gate_code,last_deferred_at_utc=excluded.last_deferred_at_utc,'
                'deferral_count=native_candidate_wait.deferral_count+1',
                (job['request_id'], time.time() + max(0.05, error.delay), error.code, utc_now()))
            queue.counter('native_candidate_gate_deferrals', 1)
        queue.metrics.count(job['provider'], 'native_candidate_gate_deferrals')
        return
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
            if job.get('evidence_contract_version') == 68:
                row = queue.db.execute('SELECT * FROM articles WHERE url=?', (job['url'],)).fetchone()
                source, identity = _question_source(queue.db, queue.registry, job['url'], 68)
                if (_candidate_head_problem(row, time.time(), registry=queue.registry,
                        layout='ettoday_visible_article_v41', evidence=68)
                        or _head(row) != job.get('frozen_target') or identity != job.get('frozen_source')
                        or _prior_ettoday_creator_conflict(queue.db, source, job['url']) != job.get('expected_conflict')):
                    raise ValueError('candidate_predecessor_changed_before_retention')
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
        # This fixed question keeps public header names/declarations, never an
        # original/raw response or article prose. Do not label names absent,
        # and do not extend private-original retention to metadata questions.
        declaration_result = (job.get('evidence_contract_version') in {46,47,48,61}
            and isinstance(result, dict) and not original_result
            and result.get('public_header_declarations_retained') is True
            and result.get('raw_or_body_retained') is False
            and result.get('raw_or_names_or_body_retained') is True
            and 'private_original' not in result)
        excerpt = result.get('ettoday_unclassified_prelude') if isinstance(result, dict) else None
        excerpt_result = (job.get('evidence_contract_version') == 62 and not original_result
            and isinstance(excerpt, dict) and isinstance(excerpt.get('text'), str)
            and 1 <= len(excerpt['text']) <= 160 and excerpt.get('possibly_prose') is True
            and excerpt.get('text_sha256') == hashlib.sha256(excerpt['text'].encode()).hexdigest()
            and excerpt.get('question') == ETTODAY_PRELUDE_EXCERPT_QUESTION
            and result.get('question') == ETTODAY_PRELUDE_EXCERPT_QUESTION
            and excerpt.get('paragraph_index') == 2
            and excerpt.get('attribution_accepted') is False
            and excerpt.get('author_or_supplier_accepted') is False
            and excerpt.get('raw_response_retained') is False and excerpt.get('full_body_retained') is False
            and result.get('body_completeness_verified') is False and result.get('identity_or_alias_accepted') is False
            and result.get('diagnostic_excerpt_retained') is True
            and result.get('raw_or_body_retained') is True and result.get('raw_or_names_or_body_retained') is True
            and result.get('raw_response_retained') is False and result.get('full_body_retained') is False
            and 'private_original' not in result and 'private_original_permit' not in result)
        predicates_result = (job.get('evidence_contract_version') == 63 and not original_result
            and _ettoday_attribution_predicates_result(result, job['url']))
        moneyweekly_excerpt_result = False
        if job.get('evidence_contract_version') == 67 and error is None:
            from downloader.news_fulltext_moneyweekly_excerpt import excerpt_result as moneyweekly_excerpt_valid
            moneyweekly_excerpt_result = not original_result and moneyweekly_excerpt_valid(result, job['url'])
            if not moneyweekly_excerpt_result:
                raise ValueError('finite_candidate_metadata_required')
        if job.get('evidence_contract_version') == 66 and error is None:
            from downloader.news_fulltext_moneyweekly_metadata import template_result
            if original_result or not template_result(result, job['url']):
                raise ValueError('finite_candidate_metadata_required')
        if job.get('evidence_contract_version') == 65 and error is None:
            from downloader.news_fulltext_moneyweekly import moneyweekly_body_geometry_result
            if original_result or not moneyweekly_body_geometry_result(result, job['url']):
                raise ValueError('finite_candidate_metadata_required')
        if job.get('evidence_contract_version') == 64 and error is None:
            from downloader.news_fulltext_moneyweekly import (
                moneyweekly_literal_navigation_route, moneyweekly_navigation_metadata_result,
            )
            if moneyweekly_literal_navigation_route(job['url']) and (original_result
                    or not moneyweekly_navigation_metadata_result(result, job['url'])):
                raise ValueError('finite_candidate_metadata_required')
        if (not isinstance(result, dict) or result.get('accepted_as_article') is not False
                or result.get('raw_or_names_or_body_retained') is not False
                and not declaration_result and not excerpt_result and not predicates_result and not moneyweekly_excerpt_result
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
                output = dict(flags, **retention_flags(json.loads(row['request_json']), result, row['state']),
                    **{key: result if key == 'result_json' else value
                       for key, value in dict(row).items() if key != 'request_json'})
                if db.execute("SELECT 1 FROM sqlite_master WHERE name='native_candidate_wait' AND type='table'").fetchone():
                    waiting = db.execute('SELECT not_before_epoch,last_gate_code,last_deferred_at_utc,deferral_count '
                                         'FROM native_candidate_wait WHERE request_id=?', (request_id,)).fetchone()
                    if waiting:
                        output['last_request_deferral'] = dict(waiting, contract_version=CANDIDATE_DEFERRAL_CONTRACT_VERSION,
                            waiting=row['state'] == 'ready' and waiting['not_before_epoch'] > time.time())
                return output
    path = root/'reviewed_repairs'/'candidate_diagnostics'/'pending'/(request_id+'.json')
    if path.exists():
        _load(path)
        return dict(flags, request_id=request_id, state='pending', publisher_requests=0)
    return dict(flags, request_id=request_id, state='unknown')
