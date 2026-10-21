"""Reviewed same-response parser candidates, never source/scope upgrades.

Shared by explicit probes and the resident news owner. Candidate facts cannot
accept bodies, change the production registry, reset HTTP state or fetch links.
"""
from __future__ import annotations

from copy import deepcopy
import hashlib
from pathlib import Path
import re

from downloader.news_fulltext_policy import MAX_LAYOUT_VERSIONS, NewsError, PolicyRegistry, extract_article


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
    if (source is None or not source.allowed or layout != 'ettoday_visible_article_v39'
            or source.embedded_article_layout != 'ettoday_visible_article_v38'):
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
    if layout == 'ettoday_visible_article_v39':
        return ettoday_candidate_source(config, registry, repo, layout)
    if layout in MAX_LAYOUT_VERSIONS:
        return max_candidate_source(config, registry, repo, layout)
    raise ValueError('reviewed_native_candidate_required')


def candidate_evidence_allowed(source, candidate, version):
    """Fixed provider/question pairs; timestamps/reasons cannot choose scope."""
    if type(version) is not int:
        return False
    if version == 1:
        return candidate.provider != 'pocket'
    if version == 2:
        return (source.provider == candidate.provider == 'videoland'
            and candidate.embedded_article_layout == 'videoland_visible_article_v4')
    if version == 3:
        return (source.provider == candidate.provider == 'pocket'
            and source.embedded_article_layout == candidate.embedded_article_layout == 'pocket_visible_article_v3'
            and source.data_fingerprint == candidate.data_fingerprint
            and source.authorization_fingerprint == candidate.authorization_fingerprint)
    if version == 4:
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


def candidate_response_metadata(response, source, candidate, *, evidence_contract_version=1):
    """Finite result for the resident owner's request, without article retention."""
    from bs4 import BeautifulSoup
    from downloader.news_fulltext import _require_article_response
    if not candidate_evidence_allowed(source, candidate, evidence_contract_version):
        raise ValueError('reviewed_candidate_evidence_contract_required')
    facts = {'contract_version': 1, 'provider': source.provider,
        'response_bytes': len(response.body), 'response_sha256': hashlib.sha256(response.body).hexdigest(),
        'status': response.status, 'accepted_as_article': False, 'raw_or_names_or_body_retained': False}
    # The production response/redirect/MIME contract applies before any parse.
    _require_article_response(response, source)
    try:
        extract_article(response.body, source, page_url=response.url)
    except NewsError as error:
        facts.update(current_error=error.code, current_failure_location=extraction_failure_location(error))
    else:
        facts['current_error'] = None
    if evidence_contract_version == 4:
        from downloader.news_fulltext_candidate_originals import QUESTION
        # The separate positive-retention guard parses the envelope once.
        # Do not additionally build the v3 anatomy that a private original
        # already preserves, or rerun the current parser as its own candidate.
        facts.update(evidence_contract_version=4, question=QUESTION, current_source_not_candidate=True)
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
