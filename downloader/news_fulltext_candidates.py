"""Reviewed same-response parser candidates, never source/scope upgrades.

Shared by explicit probes and the resident news owner. Candidate facts cannot
accept bodies, change the production registry, reset HTTP state or fetch links.
"""
from __future__ import annotations

from copy import deepcopy
import hashlib
from pathlib import Path
import re

from downloader.news_fulltext_policy import BASTILLE_LAYOUT_VERSIONS, MAX_LAYOUT_VERSIONS, NewsError, PolicyRegistry, extract_article
from downloader.news_fulltext_bastillepost import BASTILLE_RESIDENT_EVIDENCE_VERSIONS
from downloader.news_fulltext_pdf import BOARD_PDF_ORIGINAL_LAYOUT, board_pdf_original_source


def extraction_failure_location(error):
    """Code-local parser sites and finite visibility facts; no traceback values."""
    parser_root = Path(__file__).resolve().parent
    sites, visibility, frame, examined = [], None, error.__traceback__, 0
    while frame is not None and examined < 32:
        code = frame.tb_frame.f_code
        path = Path(code.co_filename)
        if (path.parent == parser_root
                and re.fullmatch(r'news_fulltext(?:_[a-z_]+)?[.]py', path.name)
                and re.fullmatch(r'[_a-zA-Z][_a-zA-Z0-9]{0,120}', code.co_name)):
            sites.append({'module': path.name, 'function': code.co_name, 'line': frame.tb_lineno})
            if path.name == 'news_fulltext_public_originals.py' and code.co_name == '_visible':
                from bs4 import Tag
                candidate = frame.tb_frame.f_locals.get('node')
                refusing = frame.tb_frame.f_locals.get('parent')
                if isinstance(candidate, Tag) and isinstance(refusing, Tag):
                    ancestors = [candidate, *candidate.parents]
                    style = re.sub(r'\s+', '', str(refusing.get('style', ''))).lower()
                    visibility = {'candidate_tag': candidate.name, 'refusing_tag': refusing.name,
                        'candidate_text_characters': len(candidate.get_text(' ', strip=True)),
                        'refusing_hidden': refusing.has_attr('hidden'),
                        'refusing_aria_hidden': str(refusing.get('aria-hidden', '')).lower() == 'true',
                        'refusing_display_none': 'display:none' in style,
                        'refusing_visibility_hidden': 'visibility:hidden' in style,
                        'within_registered_cnyes_ad_slot': any(str(n.get('id', '')).startswith(
                            'div-gpt-ad-cnyes_news_article_') for n in ancestors if getattr(n, 'attrs', None))}
        frame = frame.tb_next
        examined += 1
    return {'contract_version': 2, 'sites': sites[-8:], 'visibility': visibility,
            'line_numbers_are_code_generation_local': True, 'source_text_or_locals_exported': False}


def max_candidate_source(config, registry, repo, layout):
    source = registry.sources.get('maxfinanciallife')
    version = MAX_LAYOUT_VERSIONS.get(layout, 0)
    predecessors = {name for name, prior in MAX_LAYOUT_VERSIONS.items() if prior < version}
    if (source is None or not source.allowed
            or version < 4 or source.embedded_article_layout not in predecessors):
        raise ValueError('reviewed_max_predecessor_candidate_required')
    candidate = deepcopy(config)
    next(p for p in candidate['providers'] if p['id'] == source.provider)['embedded_article_layout'] = layout
    result = PolicyRegistry(candidate, base=repo).sources[source.provider]
    if (not result.allowed or result.authorization_fingerprint != source.authorization_fingerprint
            or result.http_capacity_source_fingerprint != (source.http_capacity_source_fingerprint or source.data_fingerprint)):
        raise ValueError('candidate_must_preserve_source_auth_and_http_identity')
    return result


def ettoday_candidate_source(config, registry, repo, layout):
    source = registry.sources.get('ettoday')
    if (source is None or not source.allowed or (layout, source.embedded_article_layout) not in {
            ('ettoday_visible_article_v39', 'ettoday_visible_article_v38'),
            ('ettoday_visible_article_v40', 'ettoday_visible_article_v39')}):
        raise ValueError('reviewed_ettoday_v38_predecessor_candidate_required')
    return _exact_candidate(config, registry, repo, source, layout)


def videoland_candidate_source(config, registry, repo, layout):
    source = registry.sources.get('videoland')
    if (source is None or not source.allowed or layout != 'videoland_visible_article_v4'
            or source.embedded_article_layout != 'videoland_visible_article_v3'):
        raise ValueError('reviewed_videoland_v3_predecessor_candidate_required')
    return _exact_candidate(config, registry, repo, source, layout)


def _exact_candidate(config, registry, repo, source, layout):
    candidate = deepcopy(config)
    next(p for p in candidate['providers'] if p['id'] == source.provider)['embedded_article_layout'] = layout
    result = PolicyRegistry(candidate, base=repo).sources[source.provider]
    if (not result.allowed or result.authorization_fingerprint != source.authorization_fingerprint
            or result.http_capacity_source_fingerprint != source.http_capacity_source_fingerprint
            or source.data_fingerprint != result.parser_repair_prior_capture_fingerprint):
        raise ValueError('candidate_must_preserve_source_auth_and_http_identity')
    return result


def native_candidate_source(config, registry, repo, layout):
    from downloader.news_fulltext_moneyweekly_excerpt import LAYOUT as MONEYWEEKLY_EXCERPT_LAYOUT
    from downloader.news_fulltext_moneyweekly_metadata import LAYOUT as MONEYWEEKLY_TEMPLATE_LAYOUT
    from downloader.news_fulltext_moneyweekly import (
        ROUTE_METADATA_LAYOUT, BODY_GEOMETRY_LAYOUT, moneyweekly_route_source, moneyweekly_body_geometry_source,
    )
    if layout in {ROUTE_METADATA_LAYOUT, BODY_GEOMETRY_LAYOUT, MONEYWEEKLY_TEMPLATE_LAYOUT, MONEYWEEKLY_EXCERPT_LAYOUT}:
        source = registry.sources.get('moneyweekly')
        if not (moneyweekly_body_geometry_source(source) if layout != ROUTE_METADATA_LAYOUT else moneyweekly_route_source(source)):
            raise ValueError('reviewed_current_moneyweekly_route_source_required')
        return source
    from downloader.news_fulltext_knews import KNEWS_SPARSE_NATIVE_LAYOUT, KNEWS_TABLE_NATIVE_LAYOUT, KNEWS_GRID_NATIVE_LAYOUT, KNEWS_MULTI_REPORTER_LAYOUT, knews_resident_source
    if layout in {KNEWS_SPARSE_NATIVE_LAYOUT, KNEWS_TABLE_NATIVE_LAYOUT, KNEWS_GRID_NATIVE_LAYOUT, KNEWS_MULTI_REPORTER_LAYOUT}:
        source = registry.sources.get('knews')
        if not knews_resident_source(source) or source.embedded_article_layout != layout:
            raise ValueError('reviewed_current_knews_source_required')
        return source
    if layout in {'gnn_native_reporter_article_v1', 'gnn_native_credited_article_v2', 'gnn_native_written_article_v3', 'gnn_native_legacy_article_v4'}:
        from downloader.news_fulltext_gnn import gnn_metadata_source
        source = registry.sources.get('gnn')
        if source is None or source.embedded_article_layout != layout or not gnn_metadata_source(source):
            raise ValueError('reviewed_current_gnn_source_required')
        return source
    if layout in {'manager_public_written_article_v1', 'manager_public_written_article_v2', 'manager_public_written_article_v3', 'manager_public_written_article_v4', 'manager_public_written_article_v5'}:
        from downloader.news_fulltext_managertoday import manager_metadata_source
        source = registry.sources.get('managertoday')
        if source is None or source.embedded_article_layout != layout or not manager_metadata_source(source):
            raise ValueError('reviewed_current_manager_source_required')
        return source
    from downloader.news_fulltext_contentplatform import (CONTENTPLATFORM_ORIGINAL_LAYOUTS,
        CONTENTPLATFORM_WRITTEN_HTML_LAYOUT, contentplatform_original_source, contentplatform_known_native_source)
    if layout in CONTENTPLATFORM_ORIGINAL_LAYOUTS or layout == CONTENTPLATFORM_WRITTEN_HTML_LAYOUT:
        source = registry.sources.get('contentplatform')
        predicate = contentplatform_known_native_source if layout == CONTENTPLATFORM_WRITTEN_HTML_LAYOUT else contentplatform_original_source
        if source is None or source.embedded_article_layout != layout or not predicate(source):
            raise ValueError('reviewed_current_contentplatform_source_required')
        return source
    from downloader.news_fulltext_gvm import (
        GVM_KNOWN_URL_LAYOUT, GVM_MEDIA_LAYOUT, GVM_TABLE_LAYOUT, GVM_WRITTEN_FORMAT_LAYOUT, GVM_STATIC_EDITOR_LAYOUT, GVM_PRESENTATION_LAYOUT,
        gvm_known_url_source, gvm_media_source, gvm_table_source, gvm_original_source, gvm_static_editor_source, gvm_presentation_source,
    )
    if layout in {GVM_KNOWN_URL_LAYOUT, GVM_MEDIA_LAYOUT, GVM_TABLE_LAYOUT, GVM_WRITTEN_FORMAT_LAYOUT, GVM_STATIC_EDITOR_LAYOUT, GVM_PRESENTATION_LAYOUT}:
        source = registry.sources.get('gvm')
        predicate = {GVM_KNOWN_URL_LAYOUT: gvm_known_url_source,
            GVM_MEDIA_LAYOUT: gvm_media_source, GVM_TABLE_LAYOUT: gvm_table_source,
            GVM_WRITTEN_FORMAT_LAYOUT: gvm_original_source, GVM_STATIC_EDITOR_LAYOUT: gvm_static_editor_source,
            GVM_PRESENTATION_LAYOUT:gvm_presentation_source}[layout]
        if source is None or not predicate(source):
            raise ValueError('reviewed_current_gvm_source_required')
        return source
    from downloader.news_fulltext_setn import INEWS_LAYOUT_VERSIONS, INEWS_PROVIDER, inews_boundary_source
    if layout in INEWS_LAYOUT_VERSIONS:
        source = registry.sources.get(INEWS_PROVIDER)
        if source is None or source.embedded_article_layout != layout or not inews_boundary_source(source):
            raise ValueError('reviewed_current_inews_source_required')
        return source
    if layout in {'upmedia_public_modern_article_v1', 'upmedia_public_modern_article_v2', 'upmedia_public_modern_article_v3', 'upmedia_public_modern_article_v4'}:
        from downloader.news_fulltext_upmedia import upmedia_evidence_source
        source = registry.sources.get('upmedia')
        if source is None or not upmedia_evidence_source(source, layout=layout):
            raise ValueError('reviewed_current_upmedia_source_required')
        return source
    if layout == 'taiwannews_completed_public_original_v3':
        from downloader.news_fulltext_taiwannews import taiwannews_resident_source
        source = registry.sources.get('taiwannews')
        if source is None or not taiwannews_resident_source(source):
            raise ValueError('reviewed_current_taiwannews_source_required')
        return source
    if layout == 'taiwannews_completed_public_original_v2':
        from downloader.news_fulltext_taiwannews import taiwannews_evidence_source
        source = registry.sources.get('taiwannews')
        if source is None or not taiwannews_evidence_source(source):
            raise ValueError('reviewed_current_taiwannews_source_required')
        return source
    if layout in {'stockfeel_visible_original_v1', 'stockfeel_visible_original_v2', 'stockfeel_visible_original_v3', 'stockfeel_visible_original_v4', 'stockfeel_visible_original_v5', 'stockfeel_visible_original_v6', 'stockfeel_visible_original_v7'}:
        from downloader.news_fulltext_stockfeel import stockfeel_boundary_source
        source = registry.sources.get('stockfeel')
        if source is None or source.embedded_article_layout != layout or not stockfeel_boundary_source(source):
            raise ValueError('reviewed_current_stockfeel_source_required')
        return source
    if layout == 'cmnews_visible_article_v1':
        source = registry.sources.get('cmnews')
        if source is None or not candidate_evidence_allowed(source, source, 1):
            raise ValueError('reviewed_current_cmnews_source_required')
        return source
    from downloader.news_fulltext_candidate_originals import GLOBALVOICES_ORIGINAL_LAYOUT, globalvoices_original_source
    if layout == GLOBALVOICES_ORIGINAL_LAYOUT:
        source = registry.sources.get('globalvoices')
        if source is None or not globalvoices_original_source(source):
            raise ValueError('reviewed_current_GlobalVoices_source_required')
        return source
    if layout == BOARD_PDF_ORIGINAL_LAYOUT:
        source = registry.sources.get('fed_board')
        if source is None or not board_pdf_original_source(source):
            raise ValueError('reviewed_current_Board_PDF_source_required')
        return source
    if BASTILLE_LAYOUT_VERSIONS.get(layout, 0) >= 2:
        source = registry.sources.get('bastillepost')
        if (source is None or not source.allowed or not source.request_enabled
                or source.embedded_article_layout != layout):
            raise ValueError('reviewed_current_bastillepost_source_required')
        return source
    if layout == 'watchmedia_visible_article_v3':
        # A fixed CURRENT-source structural question, not a parser/rights
        # upgrade. One durable observation key per missing head and identity.
        source = registry.sources.get('watchmedia')
        if (source is None or not source.allowed or not source.request_enabled
                or source.embedded_article_layout != layout):
            raise ValueError('reviewed_current_watchmedia_source_required')
        return source
    pocket = re.fullmatch(r'pocket_visible_article_v([0-9]{1,2})', layout)
    if layout == 'pocket_visible_article_v3' or pocket and int(pocket[1]) >= 5:
        # Explicit evidence contract 3 inspects the CURRENT source, not an
        # invented v4 candidate or a changed authorization/capture policy.
        source = registry.sources.get('pocket')
        if source is None or not source.allowed or source.embedded_article_layout != layout:
            raise ValueError('reviewed_current_pocket_source_required')
        return source
    if layout == 'videoland_visible_article_v4':
        return videoland_candidate_source(config, registry, repo, layout)
    if layout in {'ettoday_visible_article_v39', 'ettoday_visible_article_v40', 'ettoday_visible_article_v41'}:
        from downloader.news_fulltext_ettoday import ettoday_resident_source
        source = registry.sources.get('ettoday')
        if source is not None and ettoday_resident_source(source, layout=layout):
            return source
        if layout == 'ettoday_visible_article_v39':
            return ettoday_candidate_source(config, registry, repo, layout)
        raise ValueError('reviewed_current_ettoday_source_required')
    if layout in MAX_LAYOUT_VERSIONS:
        return max_candidate_source(config, registry, repo, layout)
    raise ValueError('reviewed_native_candidate_required')


def candidate_evidence_allowed(source, candidate, version):
    """Fixed provider/question pairs; timestamps/reasons cannot choose scope."""
    if type(version) is not int:
        return False
    if version in {64, 65, 66, 67} or source.provider == 'moneyweekly' or candidate.provider == 'moneyweekly':
        from downloader.news_fulltext_moneyweekly import moneyweekly_route_source, moneyweekly_body_geometry_source
        return bool(source == candidate and (version == 64 and moneyweekly_route_source(source)
            or version in {65, 66, 67} and moneyweekly_body_geometry_source(source)))
    if version in {56, 57} or source.provider == 'knews' or candidate.provider == 'knews':
        from downloader.news_fulltext_knews import KNEWS_GRID_NATIVE_LAYOUT, knews_resident_source
        return bool(version in {56, 57} and source == candidate and knews_resident_source(source)
            and (version != 57 or source.embedded_article_layout == KNEWS_GRID_NATIVE_LAYOUT))
    if version in {48,49,50} or source.provider == 'gnn' or candidate.provider == 'gnn':
        from downloader.news_fulltext_gnn import GNN_LEGACY_LAYOUT, gnn_metadata_source
        return bool(version in {48,49,50} and source == candidate and gnn_metadata_source(source)
            and (version != 50 or source.embedded_article_layout == GNN_LEGACY_LAYOUT))
    if version in {46,47,58,59} or source.provider == 'managertoday' or candidate.provider == 'managertoday':
        from downloader.news_fulltext_managertoday import manager_metadata_source
        return bool(version in {46,47,58,59} and source == candidate and manager_metadata_source(source)
            and (version != 58 or source.embedded_article_layout=='manager_public_written_article_v4')
            and (version != 59 or source.embedded_article_layout=='manager_public_written_article_v5'))
    from downloader.news_fulltext_contentplatform import CONTENTPLATFORM_EVIDENCE_VERSIONS
    if version == 55:
        from downloader.news_fulltext_contentplatform import CONTENTPLATFORM_WRITTEN_HTML_LAYOUT, contentplatform_known_native_source
        return bool(source == candidate and contentplatform_known_native_source(source)
            and source.embedded_article_layout == CONTENTPLATFORM_WRITTEN_HTML_LAYOUT)
    if version in CONTENTPLATFORM_EVIDENCE_VERSIONS or source.provider == 'contentplatform' or candidate.provider == 'contentplatform':
        from downloader.news_fulltext_contentplatform import contentplatform_original_source
        return bool(version in CONTENTPLATFORM_EVIDENCE_VERSIONS and source == candidate and contentplatform_original_source(source))
    if version in {34, 35, 36, 37, 40, 41, 42} or source.provider == 'gvm' or candidate.provider == 'gvm':
        from downloader.news_fulltext_gvm import gvm_known_url_source, gvm_media_source, gvm_table_source, gvm_original_source, gvm_static_editor_source, gvm_presentation_source
        return bool(source == candidate and (version in {34, 35} and gvm_known_url_source(source)
            or version == 36 and gvm_media_source(source) or version == 37 and gvm_table_source(source)
            or version == 40 and gvm_original_source(source) or version == 41 and gvm_static_editor_source(source)
            or version == 42 and gvm_presentation_source(source)))
    if version in {27, 28, 29, 30, 31, 32, 33, 51, 52, 60, 61, 62, 63, 68} or source == candidate and source.provider == 'ettoday':
        from downloader.news_fulltext_ettoday import ettoday_resident_source
        return bool(source == candidate and (
            version in {61, 62, 63, 68} and ettoday_resident_source(source, layout='ettoday_visible_article_v41')
            or version == 60 and ettoday_resident_source(source, layout='ettoday_visible_article_v40')
            or version in {27, 28, 29, 30, 31, 32, 33, 51, 52} and ettoday_resident_source(source)))
    from downloader.news_fulltext_setn import INEWS_PROVIDER, inews_boundary_source
    if source.provider == INEWS_PROVIDER or candidate.provider == INEWS_PROVIDER or version in {24, 25, 26, 38, 39}:
        return bool(source == candidate and inews_boundary_source(source) and (
            version in {24, 25, 26}
            or version in {38, 39} and source.embedded_article_layout == 'setn_inews_native_article_v8'))
    if source.provider == 'upmedia' or candidate.provider == 'upmedia' or version in {20, 21, 22, 23}:
        from downloader.news_fulltext_upmedia import upmedia_evidence_source
        layout = source.embedded_article_layout
        return bool(source == candidate and (version == 20 or version == 21 and layout == 'upmedia_public_modern_article_v1'
            or version == 22 and layout == 'upmedia_public_modern_article_v2'
            or version == 23 and layout == 'upmedia_public_modern_article_v3')
            and upmedia_evidence_source(source, layout=layout))
    if source.provider == 'taiwannews' or candidate.provider == 'taiwannews' or version in {16, 17, 18}:
        from downloader.news_fulltext_taiwannews import taiwannews_evidence_source, taiwannews_resident_source
        return bool(source == candidate and (
            version in {16, 17} and taiwannews_evidence_source(source)
            or version == 18 and taiwannews_resident_source(source)))
    if source.provider == 'stockfeel' or candidate.provider == 'stockfeel':
        from downloader.news_fulltext_stockfeel import stockfeel_boundary_source
        return bool(version in {14, 15} and source == candidate and stockfeel_boundary_source(source))
    if version in {14, 15}:
        return False
    if source.provider == 'cmnews' or candidate.provider == 'cmnews':
        # Distinct fixed questions on the CURRENT registered source;
        # none can accept bodies or substitute capture/authorization.
        return bool(version in {1, 19, 54} and source == candidate and source.provider == 'cmnews'
            and source.allowed and source.request_enabled and source.domains == ('cmnews.com.tw',)
            and source.embedded_article_layout == 'cmnews_visible_article_v1'
            and source.authorization.get('kind') == 'personal_research')
    if version in {19,54}:
        return False
    if version == 13:
        from downloader.news_fulltext_candidate_originals import globalvoices_original_source
        return (globalvoices_original_source(source) and globalvoices_original_source(candidate)
            and source.data_fingerprint == candidate.data_fingerprint
            and source.authorization_fingerprint == candidate.authorization_fingerprint
            and source.http_capacity_source_fingerprint == candidate.http_capacity_source_fingerprint)
    if source.provider == 'globalvoices' or candidate.provider == 'globalvoices':
        return False
    if version == 12:
        return (board_pdf_original_source(source) and board_pdf_original_source(candidate)
            and source.data_fingerprint == candidate.data_fingerprint
            and source.authorization_fingerprint == candidate.authorization_fingerprint
            and source.http_capacity_source_fingerprint == candidate.http_capacity_source_fingerprint)
    if source.provider == 'fed_board' or candidate.provider == 'fed_board':
        return False
    if version in BASTILLE_RESIDENT_EVIDENCE_VERSIONS:
        return (source.provider == candidate.provider == 'bastillepost'
            and source.request_enabled and source.allowed
            and source.embedded_article_layout == candidate.embedded_article_layout
            and BASTILLE_LAYOUT_VERSIONS.get(source.embedded_article_layout, 0) >= (6 if version == 11 else 2)
            and source.data_fingerprint == candidate.data_fingerprint
            and source.authorization_fingerprint == candidate.authorization_fingerprint)
    if version == 6:
        return (source.provider == candidate.provider == 'watchmedia'
            and source.request_enabled and source.allowed
            and source.embedded_article_layout == candidate.embedded_article_layout == 'watchmedia_visible_article_v3'
            and source.data_fingerprint == candidate.data_fingerprint
            and source.authorization_fingerprint == candidate.authorization_fingerprint)
    if version == 1:
        return candidate.provider not in {'pocket', 'bastillepost'}
    if version == 2:
        return (source.provider == candidate.provider == 'videoland'
            and candidate.embedded_article_layout == 'videoland_visible_article_v4')
    if version == 3:
        return (source.provider == candidate.provider == 'pocket'
            and source.embedded_article_layout == candidate.embedded_article_layout == 'pocket_visible_article_v3'
            and source.data_fingerprint == candidate.data_fingerprint
            and source.authorization_fingerprint == candidate.authorization_fingerprint)
    if version in {4, 5, 7}:
        layout = re.fullmatch(r'pocket_visible_article_v([0-9]{1,2})', source.embedded_article_layout)
        return bool(layout and int(layout[1]) >= 5
            and source.provider == candidate.provider == 'pocket'
            and source.embedded_article_layout == candidate.embedded_article_layout
            and source.data_fingerprint == candidate.data_fingerprint
            and source.authorization_fingerprint == candidate.authorization_fingerprint)
    return False


def compare_native_candidate(payload, source, candidate, *, page_url):
    """One envelope; no raw/body export, requeue or second GET."""
    result = {'contract_version': 1, 'current_layout': source.embedded_article_layout,
        'candidate_layout': candidate.embedded_article_layout, 'candidate_fingerprint': candidate.data_fingerprint,
        'same_response_sha256': hashlib.sha256(payload).hexdigest(), 'accepted_as_article': False,
        'raw_response_retained': False, 'body_text_exported': False, 'production_queue_writes': False,
        'additional_publisher_requests': 0}
    try:
        article = extract_article(payload, candidate, page_url=page_url)
    except NewsError as error:
        result.update(parse_success=False, error=error.code, failure_location=extraction_failure_location(error))
    else:
        result.update(parse_success=True, body_characters=len(article['body']),
            **{k: article.get(k) for k in ('body_sha256', 'body_completeness', 'authors_json',
                'published_at_raw', 'modified_at_raw', 'historical_point_in_time')})
        if source.provider == 'videoland':
            result.pop('authors_json', None)
    return result


def candidate_response_metadata(response, source, candidate, *, evidence_contract_version=1, requested_url=None,
                                expected_conflict=None):
    """Finite result for the resident owner's request, without article retention."""
    from bs4 import BeautifulSoup
    from downloader.news_fulltext import _require_article_response
    if not candidate_evidence_allowed(source, candidate, evidence_contract_version):
        raise ValueError('reviewed_candidate_evidence_contract_required')
    if evidence_contract_version == 68:
        from downloader.news_fulltext_candidate_originals import (
            ETTODAY_CONFLICT_ORIGINAL_QUESTION, private_ettoday_conflict_original_permit)
        _require_article_response(response, source)
        facts = dict(contract_version=1, evidence_contract_version=68, provider=source.provider,
            question=ETTODAY_CONFLICT_ORIGINAL_QUESTION, status=response.status,
            requested_url=requested_url, final_url=response.url, redirect_count=len(response.redirects),
            response_bytes=len(response.body), response_sha256=hashlib.sha256(response.body).hexdigest(),
            accepted_as_article=False, raw_or_names_or_body_retained=False,
            current_source_not_candidate=True, body_completeness_verified=False,
            identity_or_alias_accepted=False)
        try:
            facts['private_original_permit'] = private_ettoday_conflict_original_permit(
                response, source, requested_url, expected_conflict)
        except NewsError as failure:
            facts['private_original_scope_error'] = failure.code
            facts['private_original_scope_failure_location'] = extraction_failure_location(failure)
        return facts
    if evidence_contract_version == 67:
        from downloader.news_fulltext_moneyweekly_excerpt import excerpt_metadata
        return excerpt_metadata(response, source, requested_url)
    if evidence_contract_version == 66:
        from downloader.news_fulltext_moneyweekly_metadata import template_metadata
        return template_metadata(response, source, requested_url)
    if evidence_contract_version == 65:
        from downloader.news_fulltext_moneyweekly import moneyweekly_body_geometry
        return moneyweekly_body_geometry(response, source, requested_url)
    if evidence_contract_version == 64:
        from downloader.news_fulltext_moneyweekly import moneyweekly_route_metadata
        return moneyweekly_route_metadata(response, source, requested_url)
    if evidence_contract_version == 63:
        from downloader.news_fulltext_ettoday import ETTODAY_ATTRIBUTION_PREDICATES_QUESTION, ettoday_finance_attribution_predicates
        _require_article_response(response, source)
        predicates = ettoday_finance_attribution_predicates(response, source, requested_url)
        return {'contract_version': 1, 'evidence_contract_version': 63, 'provider': source.provider,
            'question': ETTODAY_ATTRIBUTION_PREDICATES_QUESTION, 'status': response.status,
            'requested_url': requested_url, 'final_url': response.url, 'redirect_count': 0,
            'response_bytes': len(response.body), 'response_sha256': hashlib.sha256(response.body).hexdigest(),
            'current_source_not_candidate': True, 'accepted_as_article': False,
            'public_schema_declarations_retained': True, 'raw_or_names_or_body_retained': True,
            'raw_or_body_retained': False, 'raw_response_retained': False, 'full_body_retained': False,
            'body_completeness_verified': False, 'identity_or_alias_accepted': False,
            'ettoday_attribution_predicates': predicates}
    if evidence_contract_version == 62:
        from downloader.news_fulltext_ettoday import ETTODAY_PRELUDE_EXCERPT_QUESTION, ettoday_finance_prelude_excerpt
        _require_article_response(response, source)
        excerpt = ettoday_finance_prelude_excerpt(response, source, requested_url)
        return {'contract_version': 1, 'evidence_contract_version': 62, 'provider': source.provider,
            'question': ETTODAY_PRELUDE_EXCERPT_QUESTION, 'status': response.status,
            'requested_url': requested_url, 'final_url': response.url, 'redirect_count': 0,
            'response_bytes': len(response.body), 'response_sha256': hashlib.sha256(response.body).hexdigest(),
            'current_source_not_candidate': True, 'accepted_as_article': False,
            'raw_or_body_retained': True, 'raw_or_names_or_body_retained': True,
            'diagnostic_excerpt_retained': True, 'raw_response_retained': False,
            'full_body_retained': False, 'body_completeness_verified': False,
            'identity_or_alias_accepted': False, 'ettoday_unclassified_prelude': excerpt}
    if evidence_contract_version == 61:
        from downloader.news_fulltext_ettoday import (
            ETTODAY_CREDIT_DECLARATION_QUESTION, ettoday_finance_credit_declarations)
        _require_article_response(response, source)
        header = ettoday_finance_credit_declarations(response, source, requested_url)
        retained = bool(header['native_credit_declarations'])
        return {'contract_version': 1, 'evidence_contract_version': 61, 'provider': source.provider,
            'question': ETTODAY_CREDIT_DECLARATION_QUESTION, 'status': response.status,
            'requested_url': requested_url, 'final_url': response.url, 'redirect_count': 0,
            'response_bytes': len(response.body), 'response_sha256': hashlib.sha256(response.body).hexdigest(),
            'current_source_not_candidate': True, 'accepted_as_article': False,
            'raw_or_body_retained': False, 'body_completeness_verified': False,
            'raw_or_names_or_body_retained': retained, 'public_header_declarations_retained': retained,
            'identity_or_alias_accepted': False, 'ettoday_native_header': header}
    if evidence_contract_version == 60:
        from downloader.news_fulltext_candidate_originals import (
            ETTODAY_CAPTION_ORIGINAL_QUESTION, private_ettoday_caption_original_permit)
        _require_article_response(response, source)
        shape = {}
        facts = dict(contract_version=1, evidence_contract_version=60, provider=source.provider,
            question=ETTODAY_CAPTION_ORIGINAL_QUESTION, status=response.status,
            requested_url=requested_url, final_url=response.url, redirect_count=len(response.redirects),
            response_bytes=len(response.body), response_sha256=hashlib.sha256(response.body).hexdigest(),
            accepted_as_article=False, raw_or_names_or_body_retained=False,
            current_source_not_candidate=True, body_completeness_verified=False,
            identity_or_alias_accepted=False, native_caption_structure=shape)
        try:
            facts['private_original_permit'] = private_ettoday_caption_original_permit(
                response, source, requested_url, shape_facts=shape)
        except NewsError as failure:
            facts['private_original_scope_error'] = failure.code
            facts['private_original_scope_failure_location'] = extraction_failure_location(failure)
        return facts
    if evidence_contract_version in {58,59}:
        from downloader.news_fulltext_managertoday import MANAGER_BODY_ORIGINAL_QUESTION, private_manager_body_original_permit
        from downloader.news_fulltext_managertoday import MANAGER_MULTIPLE_ORIGINAL_QUESTION,private_manager_multiple_original_permit
        question,permit = ((MANAGER_MULTIPLE_ORIGINAL_QUESTION,private_manager_multiple_original_permit)
            if evidence_contract_version==59 else (MANAGER_BODY_ORIGINAL_QUESTION,private_manager_body_original_permit))
        _require_article_response(response,source)
        facts = dict(contract_version=1,evidence_contract_version=evidence_contract_version,provider=source.provider,
            question=question,status=response.status,requested_url=requested_url,final_url=response.url,
            response_bytes=len(response.body),response_sha256=hashlib.sha256(response.body).hexdigest(),
            accepted_as_article=False,raw_or_names_or_body_retained=False,current_source_not_candidate=True,
            body_completeness_verified=False,identity_or_alias_accepted=False)
        try:
            facts['private_original_permit'] = permit(response,source,requested_url)
        except NewsError as failure:
            facts['private_original_scope_error'] = failure.code
            facts['private_original_scope_failure_location'] = extraction_failure_location(failure)
        return facts
    if evidence_contract_version == 57:
        from downloader.news_fulltext_knews import KNEWS_HEADER_SHAPE_QUESTION, knews_missing_header_metadata
        _require_article_response(response, source)
        return {'contract_version': 1, 'evidence_contract_version': 57, 'provider': source.provider,
            'question': KNEWS_HEADER_SHAPE_QUESTION, 'status': response.status,
            'requested_url': requested_url, 'final_url': response.url,
            'response_bytes': len(response.body), 'response_sha256': hashlib.sha256(response.body).hexdigest(),
            'accepted_as_article': False, 'raw_or_names_or_body_retained': False,
            'current_source_not_candidate': True, 'body_completeness_verified': False,
            'identity_or_alias_accepted': False,
            'native_header_structure': knews_missing_header_metadata(response, source, requested_url)}
    if evidence_contract_version == 56:
        from downloader.news_fulltext_knews import (
            KNEWS_FAILURE_ORIGINAL_QUESTION, private_knews_failure_original_permit)
        _require_article_response(response, source)
        facts = {'contract_version': 1, 'evidence_contract_version': 56, 'provider': source.provider,
            'question': KNEWS_FAILURE_ORIGINAL_QUESTION, 'status': response.status,
            'requested_url': requested_url, 'final_url': response.url,
            'response_bytes': len(response.body), 'response_sha256': hashlib.sha256(response.body).hexdigest(),
            'accepted_as_article': False, 'raw_or_names_or_body_retained': False,
            'current_source_not_candidate': True, 'body_completeness_verified': False,
            'identity_or_alias_accepted': False}
        try:
            facts['private_original_permit'] = private_knews_failure_original_permit(response, source, requested_url)
        except NewsError as failure:
            facts['private_original_scope_error'] = failure.code
            facts['private_original_scope_failure_location'] = extraction_failure_location(failure)
        return facts
    if evidence_contract_version in {51, 52}:
        from downloader.news_fulltext_candidate_originals import (
            private_ettoday_credit_original_permit, private_ettoday_disagreement_original_permit,
            ETTODAY_DISAGREEMENT_ORIGINAL_QUESTION)
        from downloader.news_fulltext_ettoday import ETTODAY_SCHEMA_RELATIONSHIP_QUESTION
        _require_article_response(response, source)
        relationships = {}
        result = {'contract_version': 1, 'evidence_contract_version': evidence_contract_version, 'provider': source.provider,
            'question': ETTODAY_SCHEMA_RELATIONSHIP_QUESTION if evidence_contract_version == 51 else ETTODAY_DISAGREEMENT_ORIGINAL_QUESTION,
            'status': response.status,
            'requested_url': requested_url, 'final_url': response.url, 'redirect_count': len(response.redirects),
            'response_bytes': len(response.body), 'response_sha256': hashlib.sha256(response.body).hexdigest(),
            'accepted_as_article': False, 'raw_or_names_or_body_retained': False,
            'current_source_not_candidate': True, 'current_parser_replayed': False,
            'body_completeness_verified': False, 'identity_or_alias_accepted': False,
            'schema_relationships': relationships}
        try:
            if evidence_contract_version == 52:
                result['private_original_permit'] = private_ettoday_disagreement_original_permit(
                    response, source, requested_url, relationship_facts=relationships)
            else:
                private_ettoday_credit_original_permit(response, source, requested_url,
                    _visible_credit=True, _replaced_images=True, _first_nonempty_line=True,
                    _canonical_author_scope=True, _relationship_facts=relationships)
        except NewsError as error:
            result['scope_error'] = error.code
            result['failure_location'] = extraction_failure_location(error)
        return result
    if evidence_contract_version == 50:
        from downloader.news_fulltext_gnn import gnn_route_metadata
        return gnn_route_metadata(response, source, requested_url)
    if evidence_contract_version == 49:
        from downloader.news_fulltext_gnn import GNN_BOUNDARY_ORIGINAL_QUESTION,private_gnn_boundary_original_permit
        _require_article_response(response,source)
        facts={'contract_version':1,'evidence_contract_version':49,'provider':source.provider,
            'question':GNN_BOUNDARY_ORIGINAL_QUESTION,'response_bytes':len(response.body),
            'response_sha256':hashlib.sha256(response.body).hexdigest(),'status':response.status,
            'requested_url':requested_url,'final_url':response.url,'current_source_not_candidate':True,
            'accepted_as_article':False,'raw_or_names_or_body_retained':False,
            'body_completeness_verified':False,'identity_or_alias_accepted':False}
        try:
            facts['private_original_permit']=private_gnn_boundary_original_permit(response,source,requested_url)
        except NewsError as failure:
            facts['private_original_scope_error']=failure.code
            facts['private_original_scope_failure_location']=extraction_failure_location(failure)
        return facts
    if evidence_contract_version == 48:
        from downloader.news_fulltext_document_metadata import public_document_identity_shape
        from downloader.news_fulltext_gnn import GNN_METADATA_QUESTION, gnn_native_metadata
        _require_article_response(response, source)
        soup = BeautifulSoup(response.body, source.html_parser)
        return {'contract_version': 1, 'evidence_contract_version': 48,
            'provider': source.provider, 'question': GNN_METADATA_QUESTION,
            'response_bytes': len(response.body), 'response_sha256': hashlib.sha256(response.body).hexdigest(),
            'status': response.status, 'requested_url': requested_url, 'final_url': response.url,
            'current_source_not_candidate': True, 'accepted_as_article': False,
            'raw_or_body_retained': False, 'body_completeness_verified': False,
            'raw_or_names_or_body_retained': True, 'public_header_declarations_retained': True,
            'identity_or_alias_accepted': False,
            'public_document_identity': public_document_identity_shape(soup),
            'gnn_native_header': gnn_native_metadata(soup)}
    if evidence_contract_version in {46,47}:
        from downloader.news_fulltext_document_metadata import public_document_identity_shape
        from downloader.news_fulltext_managertoday import MANAGER_METADATA_QUESTION, MANAGER_PLAIN_CREDIT_QUESTION, manager_native_metadata
        _require_article_response(response, source)
        soup = BeautifulSoup(response.body, source.html_parser)
        return {'contract_version': 1, 'evidence_contract_version': evidence_contract_version,
            'provider': source.provider, 'question': MANAGER_METADATA_QUESTION
                if evidence_contract_version==46 else MANAGER_PLAIN_CREDIT_QUESTION,
            'response_bytes': len(response.body), 'response_sha256': hashlib.sha256(response.body).hexdigest(),
            'status': response.status, 'requested_url': requested_url, 'final_url': response.url,
            'current_source_not_candidate': True, 'accepted_as_article': False,
            'raw_or_body_retained': False, 'body_completeness_verified': False,
            'raw_or_names_or_body_retained': True, 'public_header_declarations_retained': True,
            'identity_or_alias_accepted': False,
            'public_document_identity': public_document_identity_shape(soup),
            'manager_native_header': manager_native_metadata(soup,plain_credit_header=evidence_contract_version==47)}
    if evidence_contract_version == 55:
        from downloader.news_fulltext_contentplatform import CONTENTPLATFORM_FAILURE_SHAPE_QUESTION, contentplatform_failure_shape
        from downloader.news_fulltext_policy import public_article_document
        _require_article_response(response,source)
        if len(response.body)>8<<20:
            raise NewsError('candidate_response_exceeds_bound')
        if response.url != requested_url or response.redirects:
            raise NewsError('candidate_literal_response_not_verified')
        facts = dict(contract_version=1,evidence_contract_version=55,provider=source.provider,
            question=CONTENTPLATFORM_FAILURE_SHAPE_QUESTION,status=response.status,
            response_bytes=len(response.body),response_sha256=hashlib.sha256(response.body).hexdigest(),
            requested_url=requested_url,final_url=response.url,current_source_not_candidate=True,
            raw_or_names_or_body_retained=False,accepted_as_article=False,
            body_completeness_verified=False,identity_or_alias_accepted=False)
        try:
            soup = public_article_document(response.body,source,page_url=requested_url)
        except NewsError as error:
            facts.update(current_native_error=error.code,failure_location=extraction_failure_location(error))
            return facts
        facts['native_failure_shape'] = contentplatform_failure_shape(soup,page_url=requested_url)
        return facts
    from downloader.news_fulltext_contentplatform import CONTENTPLATFORM_EVIDENCE_VERSIONS
    if evidence_contract_version in CONTENTPLATFORM_EVIDENCE_VERSIONS:
        from downloader.news_fulltext_candidate_originals import (
            private_contentplatform_original_permit, private_contentplatform_root_original_permit,
            private_contentplatform_image_original_permit, private_contentplatform_prefix_original_permit)
        from downloader.news_fulltext_contentplatform import CONTENTPLATFORM_EVIDENCE_QUESTIONS
        question = CONTENTPLATFORM_EVIDENCE_QUESTIONS[evidence_contract_version]
        verify = {43: private_contentplatform_original_permit, 44: private_contentplatform_root_original_permit,
            45: private_contentplatform_image_original_permit,
            53: private_contentplatform_prefix_original_permit}[evidence_contract_version]
        _require_article_response(response, source)
        facts = {'contract_version': 1, 'evidence_contract_version': evidence_contract_version,
            'provider': source.provider, 'question': question,
            'response_bytes': len(response.body), 'response_sha256': hashlib.sha256(response.body).hexdigest(),
            'status': response.status, 'requested_url': requested_url, 'final_url': response.url,
            'current_source_not_candidate': True, 'accepted_as_article': False,
            'raw_or_names_or_body_retained': False, 'body_completeness_verified': False,
            'identity_or_alias_accepted': False}
        try:
            permit = verify(response, source, requested_url)
        except NewsError as failure:
            facts['private_original_scope_error'] = failure.code
            facts['private_original_scope_failure_location'] = extraction_failure_location(failure)
            context = getattr(failure, 'private_scope_facts', {})
            for key in ('native_root_shape', 'native_image_credit_shape', 'native_header_failure_context',
                    'native_prefix_credit_shape'):
                if key in context:
                    facts[key] = context[key]
        else:
            facts['private_original_permit'] = permit
            shape_key = {44: 'native_root_shape', 45: 'native_image_credit_shape',
                53: 'native_prefix_credit_shape'}.get(evidence_contract_version)
            if shape_key:
                facts[shape_key] = {k: v for k, v in permit['native_permit'].items()
                    if k != 'signed_reporter_and_explicit_clocks_verified'}
        return facts
    if evidence_contract_version in {40, 41, 42}:
        from downloader.news_fulltext_candidate_originals import GVM_ORIGINAL_QUESTION, private_gvm_original_permit, private_gvm_header_original_permit, private_gvm_multiple_credit_original_permit
        from downloader.news_fulltext_gvm import GVM_HEADER_QUESTION, GVM_MULTIPLE_CREDIT_QUESTION
        question,verify={40:(GVM_ORIGINAL_QUESTION,private_gvm_original_permit),
            41:(GVM_HEADER_QUESTION,private_gvm_header_original_permit),
            42:(GVM_MULTIPLE_CREDIT_QUESTION,private_gvm_multiple_credit_original_permit)}[evidence_contract_version]
        _require_article_response(response, source)
        try:
            permit, context = verify(response, source, requested_url, include_context=True)
        except NewsError as failure:
            if not hasattr(failure, 'private_scope_facts'):
                raise
            permit = None
            context = {**failure.private_scope_facts, 'private_original_scope_error':failure.code}
        facts = {'contract_version':1, 'evidence_contract_version':evidence_contract_version, 'provider':source.provider,
            'question':question, 'response_bytes':len(response.body),
            'response_sha256':hashlib.sha256(response.body).hexdigest(), 'status':response.status,
            'requested_url':requested_url, 'final_url':response.url, 'redirect_count':0,
            'current_source_not_candidate':True, 'current_parser_replayed':True,
            'accepted_as_article':False, 'raw_or_names_or_body_retained':False,
            'body_completeness_verified':False, 'identity_or_alias_accepted':False, **context}
        if permit is not None:
            facts['private_original_permit'] = permit
        return facts
    if evidence_contract_version in {34, 35, 36, 37}:
        from downloader.news_fulltext_policy import public_article_document
        from downloader.news_fulltext_gvm import (
            GVM_PATH_PATTERN, GVM_BOUNDARY_QUESTION, GVM_EDITOR_QUESTION, GVM_REMAINING_QUESTION,
            GVM_V4_FAILURE_QUESTION, gvm_boundary_metadata, gvm_editor_grammar_metadata,
            gvm_public_native_article, gvm_failure_context,
        )
        from urllib.parse import urlsplit
        _require_article_response(response, source)
        parts = urlsplit(requested_url or '')
        if (parts.scheme != 'https' or parts.netloc != 'www.gvm.com.tw'
                or parts.query or parts.fragment or not re.fullmatch(GVM_PATH_PATTERN, parts.path)
                or response.url != requested_url or response.redirects or not 0 < len(response.body) <= 2 << 20):
            raise NewsError('candidate_literal_response_not_verified')
        document = public_article_document(response.body, source, page_url=response.url)
        if evidence_contract_version == 37:
            try:
                gvm_public_native_article(document, page_url=response.url, version=4)
            except NewsError as error:
                context = gvm_failure_context(error)
                location = extraction_failure_location(error)
            else:
                context = {'contract_version': 1, 'current_native_error': None,
                    'accepted_as_article': False, 'raw_or_names_or_body_retained': False}
                location = None
            return {'contract_version': 1, 'evidence_contract_version': 37, 'provider': source.provider,
                'question': GVM_V4_FAILURE_QUESTION, 'response_bytes': len(response.body),
                'response_sha256': hashlib.sha256(response.body).hexdigest(), 'status': response.status,
                'requested_url': requested_url, 'final_url': response.url, 'redirect_count': 0,
                'current_source_not_candidate': True, 'current_parser_replayed': True,
                'accepted_as_article': False, 'raw_or_names_or_body_retained': False,
                'body_completeness_verified': False, 'identity_or_alias_accepted': False,
                'failure_location': location, 'gvm_received_failure_context': context}
        boundary = evidence_contract_version == 34
        remaining = evidence_contract_version == 36
        return {'contract_version': 1, 'evidence_contract_version': evidence_contract_version, 'provider': source.provider,
            'question': GVM_REMAINING_QUESTION if remaining else GVM_BOUNDARY_QUESTION if boundary else GVM_EDITOR_QUESTION,
            'response_bytes': len(response.body),
            'response_sha256': hashlib.sha256(response.body).hexdigest(), 'status': response.status,
            'requested_url': requested_url, 'final_url': response.url, 'redirect_count': 0,
            'accepted_as_article': False, 'raw_or_names_or_body_retained': False,
            'current_source_not_candidate': True, 'current_parser_replayed': True,
            'body_completeness_verified': False, 'identity_or_alias_accepted': False,
            **({'gvm_native_boundary':gvm_boundary_metadata(document,page_url=response.url,version=3),
                'gvm_editor_grammar':gvm_editor_grammar_metadata(document,page_url=response.url,version=3)} if remaining
                else {'gvm_native_boundary':gvm_boundary_metadata(document,page_url=response.url)} if boundary
                else {'gvm_editor_grammar':gvm_editor_grammar_metadata(document,page_url=response.url)})}
    if evidence_contract_version in {27, 28, 29, 30, 31, 32, 33}:
        from downloader.news_fulltext_policy import public_article_document
        from downloader.news_fulltext_ettoday import (
            ETTODAY_BOUNDARY_QUESTION, ettoday_literal_diagnostic_url, ettoday_resident_boundary_metadata)
        _require_article_response(response, source)
        if (not ettoday_literal_diagnostic_url(requested_url,
                allow_house='house.ettoday.net' in source.reviewed_article_subsites) or response.url != requested_url
                or response.redirects or not 0 < len(response.body) <= 2 << 20):
            raise NewsError('candidate_literal_response_not_verified')
        if evidence_contract_version in {28, 29, 30, 31, 32, 33}:
            from downloader.news_fulltext_candidate_originals import (
                ETTODAY_CREDIT_ORIGINAL_QUESTION, ETTODAY_INLINE_ORIGINAL_QUESTION,
                ETTODAY_VISIBLE_ORIGINAL_QUESTION, ETTODAY_IMAGE_ORIGINAL_QUESTION, ETTODAY_NONEMPTY_ORIGINAL_QUESTION,
                ETTODAY_CANONICAL_ORIGINAL_QUESTION, private_ettoday_canonical_credit_original_permit,
                private_ettoday_credit_original_permit,
                private_ettoday_inline_credit_original_permit, private_ettoday_visible_credit_original_permit,
                private_ettoday_image_credit_original_permit,private_ettoday_nonempty_credit_original_permit)
            question, verify = {
                28:(ETTODAY_CREDIT_ORIGINAL_QUESTION,private_ettoday_credit_original_permit),
                29:(ETTODAY_INLINE_ORIGINAL_QUESTION,private_ettoday_inline_credit_original_permit),
                30:(ETTODAY_VISIBLE_ORIGINAL_QUESTION,private_ettoday_visible_credit_original_permit),
                31:(ETTODAY_IMAGE_ORIGINAL_QUESTION,private_ettoday_image_credit_original_permit),
                32:(ETTODAY_NONEMPTY_ORIGINAL_QUESTION,private_ettoday_nonempty_credit_original_permit),
                33:(ETTODAY_CANONICAL_ORIGINAL_QUESTION,private_ettoday_canonical_credit_original_permit),
            }[evidence_contract_version]
            try:
                permit = verify(response, source, requested_url)
            except NewsError as exc:
                if evidence_contract_version not in {30,31,32,33} or not hasattr(exc,'private_scope_facts'):
                    raise
                permit = None
                refused = {'private_original_scope_error':exc.code,'visible_credit_scope':exc.private_scope_facts}
                if evidence_contract_version == 33:
                    refused['failure_location'] = extraction_failure_location(exc)
                    stage = getattr(exc,'ettoday_attribution_failure_stage',None)
                    if stage in {'bare_reporter_creator_mismatch','named_author_schema_mismatch'}:
                        refused['attribution_failure_stage'] = stage
            else:
                refused = {}
            result = {'contract_version':1, 'evidence_contract_version':evidence_contract_version, 'provider':source.provider,
                'question':question,
                'response_bytes':len(response.body),
                'response_sha256':permit['response_sha256'] if permit else hashlib.sha256(response.body).hexdigest(),
                'status':response.status,
                'requested_url':requested_url, 'final_url':response.url, 'redirect_count':0,
                'accepted_as_article':False, 'raw_or_names_or_body_retained':False,
                'current_source_not_candidate':True, 'current_parser_replayed':False,
                'body_completeness_verified':False, 'identity_or_alias_accepted':False, **refused}
            if permit is not None:
                result['private_original_permit'] = permit
            return result
        document = public_article_document(response.body, source, page_url=response.url)
        return {'contract_version':1, 'evidence_contract_version':27, 'provider':source.provider,
            'question':ETTODAY_BOUNDARY_QUESTION, 'response_bytes':len(response.body),
            'response_sha256':hashlib.sha256(response.body).hexdigest(), 'status':response.status,
            'requested_url':requested_url, 'final_url':response.url, 'redirect_count':0,
            'accepted_as_article':False, 'raw_or_names_or_body_retained':False,
            'current_source_not_candidate':True, 'current_parser_replayed':False,
            'body_completeness_verified':False, 'identity_or_alias_accepted':False,
            'ettoday_native_boundary':ettoday_resident_boundary_metadata(document, page_url=response.url,
                allow_house='house.ettoday.net' in source.reviewed_article_subsites)}
    if evidence_contract_version in {24, 25, 26, 38, 39}:
        from downloader.news_fulltext_policy import public_article_document
        from downloader.news_fulltext_setn import (
            INEWS_BOUNDARY_QUESTION, inews_literal_article_url, inews_resident_boundary_metadata)
        _require_article_response(response, source)
        if (not inews_literal_article_url(requested_url) or response.url != requested_url
                or response.redirects or not 0 < len(response.body) <= 2 << 20):
            raise NewsError('candidate_literal_response_not_verified')
        permit = None
        if evidence_contract_version in {25, 26, 38, 39}:
            from downloader.news_fulltext_candidate_originals import (
                private_inews_original_permit, private_inews_header_original_permit,
                private_inews_body_original_permit, private_inews_short_credit_original_permit)
            verify = {25: private_inews_original_permit, 26: private_inews_header_original_permit,
                38: private_inews_body_original_permit, 39: private_inews_short_credit_original_permit}[evidence_contract_version]
            try:
                permit, document = verify(response, source, requested_url, include_document=True)
            except NewsError as error:
                if evidence_contract_version not in {38, 39}:
                    raise
                # A negative scope observation remains metadata only. Do not
                # discard the short/empty geometry or automatically retry it.
                document = public_article_document(response.body, source, page_url=response.url)
                scope_error = error.code
        else:
            document = public_article_document(response.body, source, page_url=response.url)
        facts = {'contract_version': 1, 'evidence_contract_version': evidence_contract_version, 'provider': source.provider,
            'question': permit['question'] if permit is not None else INEWS_BOUNDARY_QUESTION,
            'response_bytes': len(response.body),
            'response_sha256': hashlib.sha256(response.body).hexdigest(), 'status': response.status,
            'requested_url': requested_url, 'final_url': response.url, 'redirect_count': 0,
            'accepted_as_article': False, 'raw_or_names_or_body_retained': False,
            'current_source_not_candidate': True, 'body_completeness_verified': False,
            'identity_or_alias_accepted': False,
            'inews_native_boundary': inews_resident_boundary_metadata(document, page_url=response.url)}
        if permit is not None:
            facts['private_original_permit'] = permit
        if evidence_contract_version in {38, 39}:
            from downloader.news_fulltext_candidate_originals import INEWS_BODY_ORIGINAL_QUESTION, INEWS_SHORT_CREDIT_ORIGINAL_QUESTION
            facts['question'] = INEWS_BODY_ORIGINAL_QUESTION if evidence_contract_version == 38 else INEWS_SHORT_CREDIT_ORIGINAL_QUESTION
            facts['native_short_body_scope_verified'] = permit is not None
            if permit is None:
                facts['private_original_scope_error'] = scope_error
        return facts
    if evidence_contract_version in {20, 21, 22, 23}:
        from urllib.parse import urlsplit
        from downloader.news_fulltext_policy import public_article_document
        from downloader.news_fulltext_upmedia import (
            UPMEDIA_BOUNDARY_QUESTION, UPMEDIA_HEADER_QUESTION, UPMEDIA_EDITOR_QUESTION, UPMEDIA_PATH_PATTERN,
            UPMEDIA_REMAINING_QUESTION, upmedia_remaining_metadata,
            upmedia_boundary_metadata, upmedia_publication_metadata, upmedia_public_modern_article, upmedia_editor_metadata,
        )
        _require_article_response(response, source)
        parts = urlsplit(requested_url or '')
        if (response.url != requested_url or response.redirects or not 0 < len(response.body) <= 2 << 20
                or parts.scheme != 'https' or parts.netloc != 'www.upmedia.mg' or parts.query or parts.fragment
                or not re.fullmatch(UPMEDIA_PATH_PATTERN, parts.path)):
            raise NewsError('candidate_literal_response_not_verified')
        document = public_article_document(response.body, source, page_url=response.url)
        header_question = evidence_contract_version == 21
        facts = {'contract_version': 1, 'evidence_contract_version': evidence_contract_version, 'provider': source.provider,
            'question': UPMEDIA_REMAINING_QUESTION if evidence_contract_version == 23 else
                UPMEDIA_EDITOR_QUESTION if evidence_contract_version == 22 else
                UPMEDIA_HEADER_QUESTION if header_question else UPMEDIA_BOUNDARY_QUESTION,
            'response_bytes': len(response.body),
            'response_sha256': hashlib.sha256(response.body).hexdigest(), 'status': response.status,
            'requested_url': requested_url, 'final_url': response.url, 'redirect_count': 0,
            'accepted_as_article': False, 'raw_or_names_or_body_retained': False,
            'current_source_not_candidate': True, 'body_completeness_verified': False,
            'identity_or_alias_accepted': False}
        if header_question:
            facts['upmedia_publication_header'] = upmedia_publication_metadata(document)
            return facts
        if evidence_contract_version == 22:
            facts['upmedia_editor_transport'] = upmedia_editor_metadata(document)
            return facts
        adapter_version = int(source.embedded_article_layout.rsplit('_v', 1)[1])
        if evidence_contract_version == 23:
            facts['upmedia_remaining_context'] = upmedia_remaining_metadata(document, raw_payload=response.body)
        else:
            facts['upmedia_native_boundary'] = upmedia_boundary_metadata(document,
                adapter_contract_version=adapter_version, raw_payload=response.body)
        try:
            upmedia_public_modern_article(document, page_url=response.url, adapter_contract_version=adapter_version,
                raw_payload=response.body)
        except NewsError as error:
            facts.update(current_native_error=error.code, failure_location=extraction_failure_location(error))
        else:
            facts['current_native_error'] = None
        return facts
    if evidence_contract_version in {16, 17, 18}:
        from downloader.news_fulltext_taiwannews import (
            TAIWANNEWS_ORIGINAL_QUESTION, TAIWANNEWS_INLINE_ORIGINAL_QUESTION, TAIWANNEWS_SINGLE_ROOT_ORIGINAL_QUESTION,
            taiwannews_original_document, taiwannews_original_permit, taiwannews_inline_original_permit,
            taiwannews_single_root_original_permit, taiwannews_boundary_metadata, taiwannews_public_original)
        _require_article_response(response, source)
        document = taiwannews_original_document(response, source, requested_url,
            evidence_contract_version=evidence_contract_version)
        question, scope_permit = {
            16: (TAIWANNEWS_ORIGINAL_QUESTION, taiwannews_original_permit),
            17: (TAIWANNEWS_INLINE_ORIGINAL_QUESTION, taiwannews_inline_original_permit),
            18: (TAIWANNEWS_SINGLE_ROOT_ORIGINAL_QUESTION, taiwannews_single_root_original_permit),
        }[evidence_contract_version]
        facts = {'contract_version': 1, 'evidence_contract_version': evidence_contract_version, 'provider': source.provider,
            'question': question,
            'response_bytes': len(response.body),
            'response_sha256': hashlib.sha256(response.body).hexdigest(), 'status': response.status,
            'requested_url': requested_url, 'final_url': response.url, 'redirect_count': len(response.redirects),
            'accepted_as_article': False, 'raw_or_names_or_body_retained': False,
            'current_source_not_candidate': True, 'body_completeness_verified': False,
            'identity_or_alias_accepted': False, 'diagnostic_representation_contract_version': 1,
            'taiwannews_native_boundary': taiwannews_boundary_metadata(document)}
        try:
            facts['private_original_permit'] = scope_permit(document, response, source, requested_url)
        except NewsError as error:
            facts.update(private_original_scope_error=error.code,
                private_original_scope_failure_location=extraction_failure_location(error))
        try:
            taiwannews_public_original(document, page_url=response.url,
                adapter_contract_version=3 if evidence_contract_version == 18 else 2)
        except NewsError as error:
            facts.update(current_native_error=error.code, failure_location=extraction_failure_location(error))
        else:
            facts['current_native_error'] = None
        return facts
    if evidence_contract_version == 15:
        from downloader.news_fulltext_candidate_originals import (
            STOCKFEEL_ORIGINAL_QUESTION, _stockfeel_original_document, _stockfeel_original_permit)
        from downloader.news_fulltext_stockfeel import (
            STOCKFEEL_LAYOUT_VERSIONS, stockfeel_boundary_metadata, stockfeel_visible_original)
        _require_article_response(response, source)
        document = _stockfeel_original_document(response, source, requested_url)
        facts = {'contract_version': 1, 'evidence_contract_version': 15, 'provider': source.provider,
            'question': STOCKFEEL_ORIGINAL_QUESTION, 'response_bytes': len(response.body),
            'response_sha256': hashlib.sha256(response.body).hexdigest(), 'status': response.status,
            'requested_url': requested_url, 'final_url': response.url, 'redirect_count': 0,
            'accepted_as_article': False, 'raw_or_names_or_body_retained': False,
            'current_source_not_candidate': True, 'body_completeness_verified': False,
            'identity_or_alias_accepted': False, 'diagnostic_representation_contract_version': 2,
            'stockfeel_native_boundary': stockfeel_boundary_metadata(document, page_url=response.url)}
        try:
            facts['private_original_permit'] = _stockfeel_original_permit(document, response, source, requested_url)
        except NewsError as error:
            # Public metadata and private raw retention have distinct gates.
            # Preserve finite same-DOM refusal facts without a second request,
            # another parse or a permit that the independent writer could use.
            facts.update(private_original_scope_error=error.code,
                private_original_scope_failure_location=extraction_failure_location(error))
        try:
            stockfeel_visible_original(document, page_url=response.url,
                adapter_contract_version=STOCKFEEL_LAYOUT_VERSIONS[source.embedded_article_layout])
        except NewsError as error:
            facts.update(current_native_error=error.code, failure_location=extraction_failure_location(error))
        else:
            facts['current_native_error'] = None
        return facts
    if evidence_contract_version == 14:
        from downloader.news_fulltext_stockfeel import (
            STOCKFEEL_BOUNDARY_QUESTION, stockfeel_boundary_metadata, stockfeel_visible_original)
        _require_article_response(response, source)
        if response.url != requested_url or response.redirects:
            raise NewsError('candidate_literal_response_not_verified')
        if len(response.body) > 2 << 20:
            raise NewsError('candidate_response_exceeds_bound')
        from downloader.news_fulltext_policy import public_article_document
        facts = {'contract_version': 1, 'evidence_contract_version': 14,
            'provider': source.provider, 'question': STOCKFEEL_BOUNDARY_QUESTION,
            'response_bytes': len(response.body), 'response_sha256': hashlib.sha256(response.body).hexdigest(),
            'status': response.status, 'requested_url': requested_url, 'final_url': response.url,
            'accepted_as_article': False, 'body_text_retained': False, 'raw_response_retained': False,
            'personal_names_retained': False, 'raw_or_names_or_body_retained': False,
            'current_source_not_candidate': True,
            'body_completeness_verified': False, 'identity_or_alias_accepted': False}
        try:
            document = public_article_document(response.body, source, page_url=response.url)
        except NewsError as error:
            facts.update(current_native_error=error.code, failure_location=extraction_failure_location(error))
            return facts
        facts['stockfeel_native_boundary'] = stockfeel_boundary_metadata(document, page_url=response.url)
        try:
            from downloader.news_fulltext_stockfeel import STOCKFEEL_LAYOUT_VERSIONS
            stockfeel_visible_original(document, page_url=response.url,
                adapter_contract_version=STOCKFEEL_LAYOUT_VERSIONS[source.embedded_article_layout])
        except NewsError as error:
            facts.update(current_native_error=error.code, failure_location=extraction_failure_location(error))
        else:
            facts['current_native_error'] = None
        return facts
    if evidence_contract_version == 13:
        from downloader.news_fulltext_candidate_originals import (
            GLOBALVOICES_ORIGINAL_QUESTION, private_globalvoices_original_permit)
        _require_article_response(response, source)
        private_globalvoices_original_permit(response, source, requested_url)
        return {'contract_version': 1, 'evidence_contract_version': 13, 'provider': source.provider,
            'question': GLOBALVOICES_ORIGINAL_QUESTION, 'response_bytes': len(response.body),
            'response_sha256': hashlib.sha256(response.body).hexdigest(), 'status': response.status,
            'requested_url': requested_url, 'final_url': response.url, 'redirect_count': 0,
            'accepted_as_article': False, 'raw_or_names_or_body_retained': False,
            'current_source_not_candidate': True, 'current_parser_replayed': False,
            'native_CC_authors_clock_scope_verified': True, 'body_completeness_verified': False,
            'third_party_media_requested': False, 'identity_or_alias_accepted': False}
    if evidence_contract_version == 12:
        from downloader.news_fulltext_pdf import BOARD_PDF_ORIGINAL_QUESTION, private_board_pdf_permit
        # Transport/status/MIME and finite registration precede retention.
        # Do not retry the failing text extractor or a PDF decoder here.
        _require_article_response(response, source)
        private_board_pdf_permit(response, source, requested_url)
        return {'contract_version': 1, 'evidence_contract_version': 12, 'provider': source.provider,
            'question': BOARD_PDF_ORIGINAL_QUESTION, 'response_bytes': len(response.body),
            'response_sha256': hashlib.sha256(response.body).hexdigest(), 'status': response.status,
            'requested_url': requested_url, 'final_url': response.url, 'redirect_count': 0,
            'accepted_as_article': False, 'raw_or_names_or_body_retained': False,
            'current_source_not_candidate': True, 'current_parser_replayed': False,
            'PDF_decoded': False, 'image_only_or_malformed_verified': False,
            'identity_or_alias_accepted': False}
    if evidence_contract_version == 7:
        from downloader.news_fulltext_pocket_scope import (
            QUESTION, require_non_school_response, non_school_metadata)
        require_non_school_response(response, requested_url)
        return {'contract_version': 1, 'evidence_contract_version': 7, 'provider': source.provider,
            'question': QUESTION, 'response_bytes': len(response.body),
            'response_sha256': hashlib.sha256(response.body).hexdigest(), 'status': response.status,
            'requested_url': requested_url, 'final_url': response.url,
            'redirect_count': len(response.redirects), 'accepted_as_article': False,
            'raw_or_names_or_body_retained': False, 'current_source_not_candidate': True,
            'current_parser_replayed': False, 'identity_or_alias_accepted': False,
            'non_school_boundary': non_school_metadata(
                BeautifulSoup(response.body, source.html_parser), page_url=response.url)}
    if evidence_contract_version == 5:
        from downloader.news_fulltext_candidate_originals import legacy_route, MAX_ORIGINAL_BYTES, original_route_version
        if not legacy_route(requested_url, adapter_contract_version=original_route_version(source.embedded_article_layout)):
            raise ValueError('literal_current_pocket_legacy_route_required')
        if len(response.body) > MAX_ORIGINAL_BYTES:
            raise NewsError('candidate_response_exceeds_bound')
    facts = {'contract_version': 1, 'provider': source.provider,
        'response_bytes': len(response.body), 'response_sha256': hashlib.sha256(response.body).hexdigest(),
        'status': response.status, 'accepted_as_article': False, 'raw_or_names_or_body_retained': False}
    # The production response/redirect/MIME contract applies before any parse.
    if evidence_contract_version != 5:
        _require_article_response(response, source)
    if source.provider == 'cmnews':
        from downloader.news_fulltext_policy import public_article_document
        from downloader.news_fulltext_publishers import cmnews_identity_metadata, cmnews_visible_article
        if len(response.body) > 2 << 20:
            raise NewsError('candidate_response_exceeds_bound')
        from downloader.news_fulltext_publishers import (CMNEWS_CANONICAL_RELATION_QUESTION,
            CMNEWS_CANONICAL_ATTRIBUTE_QUESTION, cmnews_canonical_relation_metadata,
            cmnews_canonical_attribute_metadata)
        facts.update(question=({19: CMNEWS_CANONICAL_RELATION_QUESTION,
            54: CMNEWS_CANONICAL_ATTRIBUTE_QUESTION}.get(evidence_contract_version,
                'cmnews_current_identity_boundary_v1')), current_source_not_candidate=True,
            current_parser_replayed=False, body_completeness_verified=False)
        if evidence_contract_version == 54:
            facts.update(evidence_contract_version=54, requested_url=requested_url, final_url=response.url,
                identity_or_alias_accepted=False)
        try:
            document = public_article_document(response.body, source, page_url=response.url)
        except NewsError as error:
            facts.update(current_native_error=error.code, failure_location=extraction_failure_location(error))
            return facts
        facts['native_identity'] = cmnews_identity_metadata(document, page_url=response.url)
        if evidence_contract_version == 54:
            if response.url != requested_url:
                facts['current_native_error'] = 'article_identity_not_verified'
                return facts
            facts['canonical_attribute'] = cmnews_canonical_attribute_metadata(document, page_url=requested_url)
            return facts
        if evidence_contract_version == 19:
            # Literal declarations only. Do not replay the completed identity
            # diagnosis or turn matching UUIDs into an accepted alias/body.
            facts['evidence_contract_version'] = 19
            facts['canonical_relation'] = cmnews_canonical_relation_metadata(document, page_url=response.url)
            facts['identity_or_alias_accepted'] = False
            return facts
        try:
            cmnews_visible_article(document, page_url=response.url)
        except NewsError as error:
            facts.update(current_native_error=error.code, failure_location=extraction_failure_location(error))
        else:
            facts['current_native_error'] = None
        return facts
    if evidence_contract_version in {8, 10}:
        if len(response.body) > 2 << 20:
            raise NewsError('candidate_response_exceeds_bound')
        from downloader.news_fulltext_bastillepost import (
            BASTILLE_WRITTEN_BOUNDARY_QUESTION, bastillepost_transport_verified,
            bastillepost_written_boundary_metadata, BASTILLE_ACTIVE_RESOURCE_QUESTION,
            bastillepost_active_resource_metadata,
        )
        version = BASTILLE_LAYOUT_VERSIONS[source.embedded_article_layout] if evidence_contract_version == 10 else 2
        if not bastillepost_transport_verified(response, requested_url, adapter_contract_version=version):
            raise NewsError('candidate_literal_native_transport_not_verified')
        question, field, observe = ((BASTILLE_ACTIVE_RESOURCE_QUESTION,'bastillepost_active_resources',bastillepost_active_resource_metadata)
            if evidence_contract_version == 10 else (BASTILLE_WRITTEN_BOUNDARY_QUESTION,'bastillepost_written_boundary',bastillepost_written_boundary_metadata))
        facts.update(question=question, current_source_not_candidate=True,
            current_parser_replayed=False, requested_url=requested_url, final_url=response.url,
            redirect_count=len(response.redirects), identity_or_alias_accepted=False,
            **{field:observe(BeautifulSoup(response.body, source.html_parser),page_url=response.url)})
        return facts
    if evidence_contract_version in {9, 11}:
        from downloader.news_fulltext_bastillepost import bastillepost_transport_verified
        from downloader.news_fulltext_candidate_originals import (
            BASTILLE_SHORT_QUESTION, BASTILLE_CONTINUATION_QUESTION, MAX_ORIGINAL_BYTES)
        if len(response.body) > MAX_ORIGINAL_BYTES:
            raise NewsError('candidate_response_exceeds_bound')
        version = 2 if evidence_contract_version == 9 else BASTILLE_LAYOUT_VERSIONS[source.embedded_article_layout]
        if not bastillepost_transport_verified(response, requested_url, adapter_contract_version=version):
            raise NewsError('candidate_literal_native_transport_not_verified')
        # The separate positive retention guard parses the native document
        # once. Do not replay a failing body rule as its own candidate oracle.
        facts.update(question=BASTILLE_SHORT_QUESTION if evidence_contract_version == 9
                else BASTILLE_CONTINUATION_QUESTION, evidence_contract_version=evidence_contract_version,
            current_source_not_candidate=True, current_parser_replayed=False,
            requested_url=requested_url, final_url=response.url, redirect_count=len(response.redirects),
            identity_or_alias_accepted=False)
        return facts
    if source.provider == 'watchmedia':
        if len(response.body) > 2 << 20:
            raise NewsError('candidate_response_exceeds_bound')
        from downloader.news_fulltext_watchmedia import watchmedia_credit_boundary_metadata
        # This is NOT comparing the current parser with itself. The failure
        # head stays unchanged; one bounded structural parse answers only the
        # scoped-credit question and exports no names or article text.
        facts.update(question=('watchmedia_literal_legacy_credit_boundary_v1' if evidence_contract_version == 6
                               else 'watchmedia_own_credit_boundary_v1'), current_source_not_candidate=True,
            current_parser_replayed=False, watchmedia_credit_boundary=watchmedia_credit_boundary_metadata(
                BeautifulSoup(response.body, source.html_parser), page_url=response.url))
        if evidence_contract_version == 6:
            facts['legacy_transport'] = {'requested_url': requested_url, 'final_url': response.url,
                'redirect_count': len(response.redirects), 'identity_or_alias_accepted': False}
        return facts
    try:
        if evidence_contract_version == 5:
            _require_article_response(response, source)
        extract_article(response.body, source, page_url=response.url)
    except NewsError as error:
        facts.update(current_error=error.code, current_failure_location=extraction_failure_location(error))
    else:
        facts['current_error'] = None
    if evidence_contract_version in {4, 5}:
        from downloader.news_fulltext_candidate_originals import (
            QUESTION, LEGACY_QUESTION, legacy_transport_verified, original_route_version,
        )
        # The separate positive-retention guard parses the envelope once.
        # Do not additionally build the v3 anatomy that a private original
        # already preserves, or rerun the current parser as its own candidate.
        facts.update(evidence_contract_version=evidence_contract_version,
            question=LEGACY_QUESTION if evidence_contract_version == 5 else QUESTION,current_source_not_candidate=True)
        if evidence_contract_version == 5:
            from dataclasses import asdict
            from downloader.news_fulltext_diagnostics import _display_url
            from downloader.news_fulltext_pocket import pocket_article_id
            route_version = original_route_version(source.embedded_article_layout)
            facts['legacy_transport'] = {'request_url':_display_url(requested_url),
                'final_url':_display_url(response.url),
                'requested_article_id':pocket_article_id(requested_url,requested=True,adapter_contract_version=route_version),
                'final_article_id':pocket_article_id(response.url,requested=True,adapter_contract_version=route_version),
                'same_ID_transport_verified':legacy_transport_verified(response,requested_url,adapter_contract_version=route_version),
                'actual_redirects':[{**asdict(hop),'from_url':_display_url(hop.from_url),
                    'to_url':_display_url(hop.to_url),'location':_display_url(hop.location)}for hop in response.redirects]}
        return facts
    if evidence_contract_version == 3:
        from downloader.news_fulltext_pocket import pocket_identity_metadata
        facts.update(evidence_contract_version=3, question='pocket_native_identity_boundary_v1',
            current_source_not_candidate=True,
            pocket_native_validation=pocket_identity_metadata(
                BeautifulSoup(response.body, source.html_parser), page_url=response.url))
        # Reusing the current-source verdict avoids parsing it twice. No
        # hypothetical candidate success or article acceptance is reported.
        return facts
    result = compare_native_candidate(response.body, source, candidate, page_url=response.url)
    result.pop('authors_json', None)  # No resident diagnostic exports names.
    facts['candidate_parser'] = result
    if source.provider == 'videoland':
        from downloader.news_fulltext_videoland import (
            videoland_description_repair_metadata, videoland_validation_metadata)
        soup = BeautifulSoup(response.body, source.html_parser)
        facts['videoland_native_validation'] = videoland_validation_metadata(soup, page_url=response.url)
        facts['videoland_description_repair_preview'] = videoland_description_repair_metadata(soup, page_url=response.url)
        if evidence_contract_version == 2:
            from downloader.news_fulltext_videoland import videoland_credit_clock_metadata
            facts['evidence_contract_version'] = 2
            facts['videoland_credit_clock_evidence'] = videoland_credit_clock_metadata(soup, page_url=response.url)
    return facts
