"""Bounded private failed-response evidence from the canonical queue owner.

No extra HTTP, worker SQLite writes, accepted bodies, browser text or exports.
General evidence retains its original two attempts per provider/UTC day.
Five static Cnyes parser stages each have two separately persisted attempts;
legacy stage witnesses are charged first. After a daily bucket is spent, the
three independently scoped raw-capable providers may reserve one supplemental
sample per fixed bucket/UTC hour, never per URL or parser fingerprint. Storage
errors consume reservations, and restarts/upgrades cannot reset them. Independently
identified free Cnyes HTML or article-licensed Global Voices HTML may retain
raw bytes; other failures keep metadata. No extra network request is made.
An explicitly configured finite missing-response cohort has its own persistent
byte/write budget, not another hourly allowance. It retains each eligible
source response once and reconciles interrupted writes without a second fetch.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import json
import re
from urllib.parse import urlsplit, urlunsplit
import uuid

from downloader.artifact_io import atomic_write_json, atomic_write_parquet, sha256_file, table_from_records
from downloader.news_fulltext_http import Response
from downloader.news_fulltext_metrics import (
    CNYES_NATIVE_BODY_FAILURE_OBSERVATION_CONTRACT_VERSION, CNYES_NATIVE_BODY_FAILURE_STAGES,
)
from downloader.news_fulltext_policy import ETTODAY_LAYOUT_VERSIONS, NewsError, normalize_url
from downloader.news_fulltext_failure_focus import RECEIPT_CONTRACT_VERSION as FOCUS_RECEIPT_CONTRACT_VERSION
from downloader.news_fulltext_failure_focus import METADATA_UPGRADE_RECEIPT_CONTRACT_VERSION


FAILURE_DIAGNOSTIC_CONTRACT_VERSION = 6
FAILURE_DIAGNOSTIC_BUDGET_CONTRACT_VERSION = 3
RAW_SCOPE_OBSERVATION_CONTRACT_VERSION = 1
LAYOUT_INDEPENDENT_RAW_SCOPE_CONTRACT_VERSION = 2
PUBLIC_METADATA_RAW_SCOPE_CONTRACT_VERSION = 3
CNYES_RAW_SCOPE_REASONS = frozenset({
    'not_cnyes_or_not_success', 'request_identity_not_verified',
    'response_identity_or_mime_not_verified', 'native_identity_not_verified',
    'excluded_article_title', 'unreviewed_query', 'canonical_identity_not_verified',
    'visible_article_layout_not_verified', 'current_article_data_not_verified',
    'current_free_payment_not_verified', 'current_publication_epoch_type_not_verified',
    'current_title_type_not_verified', 'current_url_identity_not_verified',
    'visible_current_title_not_verified', 'schema_json_not_verified',
    'matching_schema_not_found', 'schema_clock_or_title_or_access_not_verified',
    'scope_check_exception', 'native_current_public_scope_proved',
    'canonical_current_public_scope_proved',
    'diagnostic_heading_identity_not_verified',
    'canonical_current_public_identity_without_body_layout_proved',
    'canonical_current_public_metadata_scope_proved',
})
DAILY_PROVIDER_ATTEMPTS = 2
HOURLY_BUCKET_ATTEMPTS = 1
HOURLY_EVIDENCE_PROVIDERS = frozenset({'cnyes', 'globalvoices', 'taiwanhot'})
IDENTITY_METADATA_CONTRACT_VERSION = 1
VALIDATION_METADATA_CODES = frozenset({
    'invalid_bulk_publication_clock', 'article_attribution_not_verified',
    'article_rights_or_scope_excluded', 'article_license_not_verified',
})


def _public_taiwanhot_scope(context, source):
    """Independent native metadata for private layout diagnosis, not a body.

    A broken/missing body selector cannot be a prerequisite for diagnosing it.
    Still require the typed known-URL scope, actual OG/title, current byline and
    real minute clock, no paid/rights override, and ORIGINAL route binding.
    """
    if (source.provider != 'taiwanhot' or source.embedded_article_layout != 'taiwanhot_visible_article_v3'
            or source.authorization.get('kind') != 'personal_research'
            or not source.known_url_only
            or context.response.status != 200
            or not re.search(r'(?:text/html|application/xhtml[+]xml)', context.response.headers.get('content-type', ''), re.I)):
        return False
    from downloader.news_fulltext_policy import public_article_document
    from downloader.news_fulltext_public_originals import taiwanhot_visible_article, taiwanhot_route_matches
    try:
        soup = public_article_document(context.response.body, source, page_url=context.response.url)
        metadata, _ = taiwanhot_visible_article(soup, page_url=context.response.url,
            native_views=True, route_views=True, require_body=False)
        return taiwanhot_route_matches(context.requested_url, metadata['source_native_article_id'], metadata['headline'])
    except (NewsError, ValueError, TypeError, AttributeError, OverflowError, RecursionError):
        return False


def article_license_validation_metadata(soup, source):
    """Article-scoped license facts, never footer substitution or a grant.

    Inspect the already received HTML. URLs omit credentials/query/fragments;
    unknown labels remain length/hash only. A visible reserved-rights label is
    evidence of that declaration, not a legal conclusion about every use.
    """
    prefix = source.required_license_url_prefix

    def link(node):
        value = str(node.get('href', ''))
        try:
            parts = urlsplit(value)
            display = (_display_url(value)[:512] if parts.scheme in {'http', 'https'}
                       and parts.netloc and not parts.username and not parts.password else None)
        except ValueError:
            display = None
        return {'display_url': display, 'original_value_sha256': hashlib.sha256(value.encode()).hexdigest(),
            'matches_current_required_prefix': bool(prefix and value.startswith(prefix))}

    selections = []
    for selector in source.license_selectors[:16]:
        nodes = soup.select(selector)
        selections.append({'selector': selector, 'node_count': len(nodes),
            'values': [link(n) for n in nodes[:16]], 'truncated': len(nodes) > 16})
    facts = {'contract_version': 1, 'configured_article_license_selections': selections,
        'selector_list_truncated': len(source.license_selectors) > 16,
        'body_text_retained': False, 'raw_response_retained': False,
        'license_or_article_acceptance_granted': False, 'additional_requests': 0}
    if source.provider == 'peopo':
        # This is the current configured article's logo, not the site footer
        # or a broad "creativecommons" search. Missing anchors need the marker
        # shape too, otherwise a reserved label and obsolete CSS look identical.
        logos = soup.select('#block-peopo-content article.node--view-mode-full > '
            '.node-pre-content .view-node-field .license-logo')
        known_labels = {'版權所有', '保留所有權利', '創用CC', 'Creative Commons'}

        def label(value):
            value = value.strip()
            return {'recognized_public_label': value if value in known_labels else None,
                'characters': len(value), 'sha256': hashlib.sha256(value.encode()).hexdigest()}

        facts['peopo_current_article_scope_count'] = len(soup.select('#block-peopo-content article.node--view-mode-full'))
        facts['peopo_current_article_logo_count'] = len(logos)
        facts['peopo_current_article_logos'] = [dict(label(n.get_text(' ', strip=True)),
            anchor_count=len(n.select('a[href]')), image_count=len(n.find_all('img')),
            image_labels=[label(str(img.get('alt', ''))) for img in n.find_all('img')[:8]]) for n in logos[:8]]
        facts['peopo_current_article_logos_truncated'] = len(logos) > 8
    return facts


def retain_identity_metadata(queue, row, error, context):
    """Bounded metadata-only witnesses, never a raw body or new request.

    Keep the original two identity witnesses/provider/UTC day unchanged.
    Four static validation outcomes have two witnesses each; they never
    refill identity/body/raw budgets, including on restart/parser upgrades.
    Reserve before DOM/I/O; no worker write, request, raw body, body permission,
    retry change, acceptance or per-URL/parser reset. Failed writes stay charged.
    """
    if error.code not in {'article_identity_not_verified', *VALIDATION_METADATA_CODES}:
        return
    provider = row['provider']
    try:
        source = queue.registry.require(row['url'])
        if (not source.allowed or not re.fullmatch(r'[a-z0-9_]+', provider)
                or context.provider != provider or source.provider != provider
                or context.source_fingerprint != source.data_fingerprint or context.requested_url != row['url']
                or queue.registry.require(context.response.url).provider != provider
                or context.response.status != 200 or len(context.response.body) > 8 << 20
                or not re.search(r'(?:text/html|application/xhtml[+]xml)', context.response.headers.get('content-type', ''), re.I)):
            return
        clock = datetime.fromisoformat(context.received_at_utc)
        if clock.tzinfo is None:
            return
        day = clock.astimezone(timezone.utc).strftime('%Y%m%d')
        key = (f'failure_validation_metadata_attempts:v1:{provider}:{error.code}:{day}'
               if error.code in VALIDATION_METADATA_CODES else
               f'failure_identity_metadata_attempts:v1:{provider}:{day}')
        with queue.db:
            previous = queue.db.execute('SELECT value FROM queue_settings WHERE key=?', (key,)).fetchone()
            attempts = int(previous[0]) if previous else 0
            if not 0 <= attempts <= DAILY_PROVIDER_ATTEMPTS:
                raise ValueError('invalid_identity_metadata_budget')
            if attempts == DAILY_PROVIDER_ATTEMPTS:
                queue.metrics.count(provider, 'failure_identity_metadata_budget_exhausted')
                return
            queue.db.execute('INSERT OR REPLACE INTO queue_settings VALUES (?,?)', (key, str(attempts + 1)))
        from bs4 import BeautifulSoup
        soup = BeautifulSoup(context.response.body, source.html_parser)
        def declarations(selector, attribute):
            nodes = soup.select(selector)
            values = []
            for node in nodes[:16]:
                value = str(node.get(attribute, ''))
                try:
                    parts = urlsplit(value)
                    display = (_display_url(value)[:512] if parts.scheme in {'http', 'https'}
                               and parts.netloc and not parts.username and not parts.password else None)
                except ValueError:
                    display = None
                values.append({'display_url': display,
                               'original_value_sha256': hashlib.sha256(value.encode()).hexdigest()})
            return {'node_count': len(nodes), 'values': values, 'truncated': len(nodes) > 16}
        titles = soup.select('h1,h2.news_title')
        facts = {'canonical': declarations('link[rel~="canonical"][href]', 'href'),
                 'og_url': declarations('meta[name="og:url"][content],meta[property="og:url"][content]', 'content'),
                 'title_nodes': len(titles),
                 'title_sha256': [hashlib.sha256(n.get_text(' ', strip=True).encode()).hexdigest() for n in titles[:16]],
                 'configured_containers': [{'selector': selector, 'nodes': len(soup.select(selector))}
                                           for selector in source.selectors[:16]]}
        if error.code == 'article_license_not_verified':
            facts['native_article_license_validation'] = article_license_validation_metadata(soup, source)
        if provider == 'taiwanhot':
            from downloader.news_fulltext_public_originals import taiwanhot_request_ids
            facts['requested_admission_candidates'] = sorted(taiwanhot_request_ids(row['url']))
            facts['response_admission_candidates'] = sorted(taiwanhot_request_ids(context.response.url))
            facts['candidates_are_not_verified_article_ids'] = True
        if provider == 'ftnn' and source.embedded_article_layout in {'ftnn_visible_article_v1', 'ftnn_visible_article_v2', 'ftnn_visible_article_v3', 'ftnn_visible_article_v4', 'ftnn_visible_article_v5', 'ftnn_visible_article_v6'}:
            # Body/parser failures must be diagnosable without another fetch
            # or retaining an unproved raw body. Compare bounded source facts;
            # no headline/byline, cookie, query, script or body text is exported.
            from downloader.news_fulltext_policy import _objects
            compact_hash = lambda value: (hashlib.sha256(re.sub(r'\s+', '', value).encode()).hexdigest()
                                           if isinstance(value, str) else None)
            schemas = []
            crumbs = []
            schema_error = False
            scripts = soup.select('script[type="application/ld+json"]')
            if len(scripts) <= 16:
                try:
                    for script in scripts:
                        if len(script.get_text()) > 65536:
                            raise ValueError('bounded metadata required')
                        for obj in _objects(json.loads(script.get_text())):
                            if obj.get('@type') == 'NewsArticle':
                                schemas.append(obj)
                            elif obj.get('@type') == 'BreadcrumbList':
                                crumbs.append(obj)
                except (ValueError, TypeError, RecursionError):
                    schema_error = True
            else:
                schema_error = True
            scoped_heads = soup.select('.news-page-all > .news-show > .news-page-top > .news-main-title > h1')
            native_version = int(source.embedded_article_layout.rsplit('_v', 1)[1])
            native_v2 = native_version >= 2
            bylines = soup.select('.news-page-all > .news-show > .news-box > .news-body > p:'
                                  + ('first-of-type' if native_v2 else 'first-child'))
            if native_version >= 3:
                from downloader.news_fulltext_ftnn import _native_byline
                bodies = soup.select('.news-page-all > .news-show > .news-box > .news-body')
                try:
                    bylines = [_native_byline(bodies[0], adapter_contract_version=native_version)] if len(bodies) == 1 else []
                except NewsError:
                    bylines = []
            releases = soup.select('.news-page-all > .news-show > .news-page-top > .news-detail-text > .ndt-left > p')
            news = schemas[0] if len(schemas) == 1 and not schema_error else {}
            author = news.get('author')
            author_name = author.get('name') if isinstance(author, dict) else None
            credit = bylines[0].get_text(' ', strip=True) if len(bylines) == 1 else ''
            release = releases[0].get_text(' ', strip=True) if len(releases) == 1 else ''
            release_pattern = r'(?:發布時間|異動時間)：[0-9]{4}/[0-9]{1,2}/[0-9]{1,2} [0-9]{1,2}:[0-9]{1,2}'
            clock_labels = []
            for node in releases[:4]:
                text = node.get_text(' ', strip=True)
                safe = bool(re.fullmatch(release_pattern, text))
                clock_labels.append({'kind': ('publication' if text.startswith('發布時間：') else 'modification')
                    if safe else 'unknown', 'text': text if safe else None,
                    'text_sha256': hashlib.sha256(text.encode()).hexdigest()})
            safe_clock = lambda value: (value if isinstance(value, str) and re.fullmatch(
                r'[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\+08:00', value) else None)
            last_crumb = (crumbs[0].get('itemListElement') if len(crumbs) == 1 else None)
            last_crumb = last_crumb[-1] if isinstance(last_crumb, list) and last_crumb else None
            last_item = last_crumb.get('item') if isinstance(last_crumb, dict) else None
            ogtitles = soup.select('meta[property="og:title"][content]')
            facts['ftnn_native_validation'] = {
                'contract_version': native_version + 1, 'schema_error': schema_error,
                'article_schema_count': len(schemas), 'breadcrumb_schema_count': len(crumbs),
                'scoped_heading_count': len(scoped_heads),
                'heading_compact_sha256': [compact_hash(n.get_text(' ', strip=True)) for n in scoped_heads[:16]],
                'schema_headline_compact_sha256': compact_hash(news.get('headline')),
                'breadcrumb_headline_compact_sha256': compact_hash(last_item.get('name')) if isinstance(last_item, dict) else None,
                'og_title_compact_sha256': [compact_hash(n.get('content')) for n in ogtitles[:16]],
                'schema_expected_og_title_compact_sha256': compact_hash(news['headline'] + ' | FTNN 新聞網')
                    if isinstance(news.get('headline'), str) else None,
                'byline_count': len(bylines), 'byline_compact_sha256': compact_hash(credit),
                'declared_author_in_visible_byline': bool(isinstance(author_name, str)
                    and author_name.strip() and re.sub(r'\s+', '', author_name) in re.sub(r'\s+', '', credit)),
                'native_reporter_byline_shape': bool(re.fullmatch(r'(?:FTNN新聞網)?'
                    + (r'(?:實習)?記者' if native_v2 else '記者')
                    + r'[^／\s]+／[^／\s]{1,16}報導', re.sub(r'\s+', '', credit))),
                'release_node_count': len(releases),
                'visible_release': release if re.fullmatch(r'發布時間：[0-9]{4}/[0-9]{1,2}/[0-9]{1,2} [0-9]{1,2}:[0-9]{1,2}', release) else None,
                'native_clock_labels': clock_labels, 'clock_labels_truncated': len(releases) > 4,
                'declared_publication': safe_clock(news.get('datePublished')),
                'declared_modification': safe_clock(news.get('dateModified')),
                'body_text_retained': False,
            }
        if provider == 'tvbs' and source.embedded_article_layout in {'tvbs_visible_article_v1', 'tvbs_visible_article_v2', 'tvbs_visible_article_v3', 'tvbs_visible_article_v4'}:
            from downloader.news_fulltext_tvbs import tvbs_validation_metadata
            facts['tvbs_native_validation'] = tvbs_validation_metadata(soup, page_url=context.response.url,
                adapter_contract_version=int(source.embedded_article_layout.rsplit('_v', 1)[1]))
        if provider == 'ettoday' and source.embedded_article_layout in ETTODAY_LAYOUT_VERSIONS:
            from downloader.news_fulltext_ettoday import ettoday_validation_metadata
            facts['ettoday_native_validation'] = ettoday_validation_metadata(soup, page_url=context.response.url,
                adapter_contract_version=int(source.embedded_article_layout.rsplit('_v', 1)[1]),
                allow_house='house.ettoday.net' in source.reviewed_article_subsites)
        stamp = clock.astimezone(timezone.utc).strftime('%Y%m%dT%H%M%S%fZ') + '-' + uuid.uuid4().hex[:8]
        target = queue.root / 'identity_metadata' / day / provider / (stamp + '.json')
        value = {'contract_version': IDENTITY_METADATA_CONTRACT_VERSION, 'provider': provider,
            'owner_id': queue.owner, 'request_url': _display_url(row['url']),
            'final_url': _display_url(context.response.url),
            'request_url_sha256': hashlib.sha256(row['url'].encode()).hexdigest(),
            'final_url_sha256': hashlib.sha256(context.response.url.encode()).hexdigest(),
            'response_sha256': hashlib.sha256(context.response.body).hexdigest(),
            'response_bytes': len(context.response.body), 'http_status': 200,
            'error_code': error.code, 'observed_at_utc': context.received_at_utc,
            'data_fingerprint': source.data_fingerprint, 'authorization_fingerprint': source.authorization_fingerprint,
            'budget_key': key, 'budget_attempt': attempts + 1, 'budget_limit': DAILY_PROVIDER_ATTEMPTS,
            'diagnostic_only': True, 'raw_response_retained': False, 'accepted_as_article': False,
            'publish': False, 'remote_training_export': False,
            'query_and_fragments_redacted': True, 'facts': facts}
        target.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        assert not target.exists(), 'preserve_original_identity_metadata'
        assert len(json.dumps(value, ensure_ascii=False).encode()) <= 65536, 'bounded_identity_metadata_required'
        atomic_write_json(target, value, durable=True)
        target.chmod(0o600)
        queue.metrics.count(provider, 'failure_identity_metadata_saved')
    except Exception:
        queue.metrics.count(provider, 'failure_identity_metadata_write_errors')


@dataclass(frozen=True)
class FailureResponse:
    response: Response
    provider: str
    source_fingerprint: str
    requested_url: str
    received_at_utc: str


def _display_url(url):
    parts = urlsplit(url)
    return urlunsplit((parts.scheme, parts.netloc, parts.path, '', ''))


def _native_stage(provider, error):
    stage = getattr(error, 'cnyes_native_body_failure_stage', None)
    return stage if provider == 'cnyes' and type(stage) is str and stage in CNYES_NATIVE_BODY_FAILURE_STAGES else None


def _stage_budget_key(provider, day, stage):
    return f'failure_diagnostic_stage_attempts:v1:{provider}:{day}:{stage}'


def _hourly_budget_key(provider, hour, bucket):
    return f'failure_diagnostic_hourly_attempts:v1:{provider}:{hour}:{bucket}'


def _seed_native_budgets(queue, provider, day):
    """Charge at most two legacy receipt witnesses, without reclassifying text.

    Unknown or missing old witnesses retain their original general charge.
    Invalid legacy evidence closes the new buckets once, never repeatedly
    reads corrupt files or permits an unverified migration to expand storage.
    This is budget accounting, not a new raw/body acceptance proof.
    """
    counts = {stage: 0 for stage in CNYES_NATIVE_BODY_FAILURE_STAGES}
    marker = f'failure_diagnostic_stage_seed:v1:{provider}:{day}'
    seeded = queue.db.execute('SELECT value FROM queue_settings WHERE key=?', (marker,)).fetchone()
    if seeded:
        return seeded[0] == 'legacy_receipt_metadata_charged'
    outcome = 'legacy_receipt_metadata_charged'
    try:
        legacy = queue.db.execute('SELECT value FROM queue_settings WHERE key=?',
            (f'failure_diagnostic_attempts:v1:{provider}:{day}',)).fetchone()
        allowance = int(legacy[0]) if legacy else 0
        if not 0 <= allowance <= DAILY_PROVIDER_ATTEMPTS:
            raise ValueError('invalid_legacy_diagnostic_budget')
        paths = []
        for path in (queue.root / 'diagnostics' / day / provider).glob('*.receipt.json'):
            paths.append(path)
            if len(paths) > DAILY_PROVIDER_ATTEMPTS:
                raise ValueError('unbounded_legacy_diagnostic_receipts')
        if len(paths) > allowance:
            raise ValueError('legacy_receipts_exceed_original_reservations')
        for path in paths:
            if path.stat().st_size > 65536:
                raise ValueError('oversized_legacy_diagnostic_receipt')
            receipt = json.loads(path.read_bytes())
            clock = datetime.fromisoformat(receipt['captured_at_utc'])
            if (receipt.get('provider') != provider or receipt.get('error_code') != 'body_not_found'
                    or receipt.get('diagnostic_only') is not True or receipt.get('accepted_as_article') is not False
                    or type(receipt.get('contract_version')) is not int or not 1 <= receipt['contract_version'] <= 6
                    or clock.tzinfo is None or clock.astimezone(timezone.utc).strftime('%Y%m%d') != day):
                raise ValueError('invalid_legacy_diagnostic_receipt')
            if receipt['contract_version'] == 5 and (receipt.get('budget_contract_version') != 2
                    or receipt.get('budget_bucket') != 'provider_general'):
                raise ValueError('unexpected_unseeded_stage_receipt')
            if receipt['contract_version'] == 6 and (receipt.get('budget_contract_version') != 3
                    or receipt.get('budget_bucket') != 'provider_general'
                    or receipt.get('budget_period_kind') != 'utc_day'
                    or receipt.get('budget_period') != day
                    or receipt.get('budget_attempt_limit') != DAILY_PROVIDER_ATTEMPTS):
                raise ValueError('unexpected_unseeded_hourly_or_stage_receipt')
            stage = receipt.get('native_body_failure_stage')
            if type(stage) is str and stage in counts:
                counts[stage] += 1
    except Exception:
        counts = dict.fromkeys(counts, DAILY_PROVIDER_ATTEMPTS)
        outcome = 'blocked_unverified_legacy_budget'
        queue.metrics.count(provider, 'failure_diagnostic_legacy_budget_seed_errors')
    for stage, attempts in counts.items():
        # Never overwrite a reservation preserved by a prior owner.
        queue.db.execute('INSERT OR IGNORE INTO queue_settings VALUES (?,?)',
            (_stage_budget_key(provider, day, stage), str(attempts)))
    queue.db.execute('INSERT INTO queue_settings VALUES (?,?)', (marker, outcome))
    return outcome == 'legacy_receipt_metadata_charged'


def _reserve_evidence(queue, provider, clock, stage):
    """Reserve before parsing/I/O; only a finite static bucket can add work.

    Old daily keys and their charges remain authoritative. Supplemental keys
    are independent of URL, source fingerprint, process and parser version.
    A closed/corrupt legacy migration cannot be reopened with an hourly key.
    """
    utc = clock.astimezone(timezone.utc)
    day, bucket = utc.strftime('%Y%m%d'), stage or 'provider_general'
    daily_key = (_stage_budget_key(provider, day, stage) if stage else
                 f'failure_diagnostic_attempts:v1:{provider}:{day}')
    with queue.db:
        previous = queue.db.execute('SELECT value FROM queue_settings WHERE key=?', (daily_key,)).fetchone()
        if stage and previous is None:
            _seed_native_budgets(queue, provider, day)
            previous = queue.db.execute('SELECT value FROM queue_settings WHERE key=?', (daily_key,)).fetchone()
        attempts = int(previous[0]) if previous else 0
        if not 0 <= attempts <= DAILY_PROVIDER_ATTEMPTS:
            raise ValueError('invalid_daily_diagnostic_budget')
        key, period, kind, limit = daily_key, day, 'utc_day', DAILY_PROVIDER_ATTEMPTS
        if attempts == DAILY_PROVIDER_ATTEMPTS:
            if (provider not in HOURLY_EVIDENCE_PROVIDERS
                    or provider == 'cnyes' and not _seed_native_budgets(queue, provider, day)):
                queue.metrics.count(provider, 'failure_diagnostic_budget_exhausted')
                return None
            # Seed the legacy daily stage accounting before any third flat
            # receipt is written. This also covers generic Cnyes failures;
            # later native-stage observations cannot mistake hourly receipts
            # for uncharged legacy general evidence.
            period, kind, limit = utc.strftime('%Y%m%dT%H'), 'utc_hour', HOURLY_BUCKET_ATTEMPTS
            key = _hourly_budget_key(provider, period, bucket)
            previous = queue.db.execute('SELECT value FROM queue_settings WHERE key=?', (key,)).fetchone()
            attempts = int(previous[0]) if previous else 0
            if not 0 <= attempts <= limit:
                raise ValueError('invalid_hourly_diagnostic_budget')
            if attempts == limit:
                queue.metrics.count(provider, 'failure_diagnostic_budget_exhausted')
                return None
        queue.db.execute('INSERT OR REPLACE INTO queue_settings VALUES (?,?)', (key, str(attempts + 1)))
    return {'budget_bucket': bucket, 'budget_period_kind': kind, 'budget_period': period,
            'budget_attempt_limit': limit, 'daily_bucket_attempt_limit': DAILY_PROVIDER_ATTEMPTS}


def _observe_raw_scope(observation, allowed, reason):
    """Static private reason only: no text, permission grant or body acceptance."""
    if reason not in CNYES_RAW_SCOPE_REASONS:
        raise ValueError('unknown_cnyes_raw_scope_reason')
    if observation is not None:
        observation.update(contract_version=RAW_SCOPE_OBSERVATION_CONTRACT_VERSION,
                           raw_scope_proved=allowed, first_failed_check=None if allowed else reason,
                           proof_basis=reason if allowed else None)
    return allowed


def _public_cnyes_scope(context, source, observation=None, *, require_body_layout=True,
                        require_heading_identity=True):
    """Public identity only, never treat the diagnostic as article acceptance."""
    if require_body_layout and not require_heading_identity:
        raise ValueError('metadata_identity_is_private_diagnostics_only')
    def observe(allowed, reason):
        result = _observe_raw_scope(observation, allowed, reason)
        if observation is not None and not require_body_layout:
            observation.update(contract_version=LAYOUT_INDEPENDENT_RAW_SCOPE_CONTRACT_VERSION,
                original_layout_check='visible_article_layout_not_verified',
                independent_identity_only=True)
            if not require_heading_identity:
                observation.update(contract_version=PUBLIC_METADATA_RAW_SCOPE_CONTRACT_VERSION,
                    original_heading_check='diagnostic_heading_identity_not_verified',
                    body_visibility_verified=False)
        return result

    if source.provider != 'cnyes' or context.response.status != 200:
        return observe(False, 'not_cnyes_or_not_success')
    parts = urlsplit(context.requested_url)
    match = re.fullmatch(r'/news/id/([0-9]+)', parts.path)
    if parts.fragment or parts.hostname not in {'news.cnyes.com', 'm.cnyes.com'} or not match:
        return observe(False, 'request_identity_not_verified')
    if not require_body_layout and (parts.scheme != 'https' or parts.netloc != 'news.cnyes.com' or parts.query):
        # This separate proof covers primary HTTPS documents only. It never
        # changes the stricter reviewed mobile/exp canonical identity contract.
        return observe(False, 'request_identity_not_verified')
    if (normalize_url(context.response.url) != normalize_url(context.requested_url)
            or context.response.headers.get('content-type', '').split(';')[0].strip().lower()
            not in {'text/html', 'application/xhtml+xml'}):
        return observe(False, 'response_identity_or_mime_not_verified')
    from bs4 import BeautifulSoup
    from downloader.news_fulltext_policy import _objects, public_article_document
    from downloader.news_fulltext_publishers import (
        _cnyes_current_article_data, _cnyes_native_article_identity,
        _cnyes_visible_article_scope, cnyes_supported_native_alias_view,
    )

    if cnyes_supported_native_alias_view(parts):
        # The exact observed exp variants have a different declared canonical
        # URI. Reuse the body's same-ID/free/current author+clock witness; do
        # not weaken that proof or require successful body extraction to
        # diagnose the body extraction failure. This consumes received bytes,
        # not another request, renderer, hidden body or extra daily allowance.
        soup = public_article_document(context.response.body, source, page_url=context.requested_url)
        identity = _cnyes_native_article_identity(soup, source, page_url=context.requested_url)
        if identity is None:
            return _observe_raw_scope(observation, False, 'native_identity_not_verified')
        if (source.excluded_article_title_pattern
                and re.search(source.excluded_article_title_pattern, identity.current_data['title'])):
            return _observe_raw_scope(observation, False, 'excluded_article_title')
        return _observe_raw_scope(observation, True, 'native_current_public_scope_proved')
    if parts.query:
        # Unknown/secret query parameters retain metadata only.
        return _observe_raw_scope(observation, False, 'unreviewed_query')

    soup = (BeautifulSoup(context.response.body, source.html_parser) if require_body_layout else
            public_article_document(context.response.body, source, page_url=context.requested_url))
    canonical = soup.select('link[rel="canonical"]')
    scope = _cnyes_visible_article_scope(soup)
    if len(canonical) != 1 or normalize_url(canonical[0].get('href', '')) != normalize_url(context.requested_url):
        return observe(False, 'canonical_identity_not_verified')
    if scope is None and require_body_layout:
        return _observe_raw_scope(observation, False, 'visible_article_layout_not_verified')
    if require_body_layout:
        heading = scope[0]
    elif require_heading_identity:
        # Identity evidence does not need to extract, recognize or accept the
        # failed body. Use only a unique DOM heading outside non-visible and
        # navigation containers, with no hidden text in its descendants.
        headings = soup.body.find_all('h1') if soup.body is not None else []
        if len(headings) != 1 or not _diagnostic_heading_identity(headings[0]):
            return observe(False, 'diagnostic_heading_identity_not_verified')
        heading = headings[0]
    else:
        # A received, already-allowed public response can be diagnosed from
        # its exact canonical/current/free/schema identity without accepting
        # any rendered/hidden body. No body or heading text supplies this proof.
        heading = None
    data = _cnyes_current_article_data(soup, int(match[1]))
    if not data:
        return observe(False, 'current_article_data_not_verified')
    if type(data.get('payment')) is not int or data['payment'] != 0:
        return observe(False, 'current_free_payment_not_verified')
    if type(data.get('publishAt')) is not int:
        return observe(False, 'current_publication_epoch_type_not_verified')
    if not isinstance(data.get('title'), str):
        return observe(False, 'current_title_type_not_verified')
    if normalize_url(data.get('url', '')) != normalize_url(context.requested_url):
        return observe(False, 'current_url_identity_not_verified')
    if (not require_body_layout and not data['title'].strip()
            or heading is not None and re.sub(r'\s+', '', heading.get_text(' ', strip=True))
            != re.sub(r'\s+', '', data['title'])):
        return observe(False, 'visible_current_title_not_verified')
    if source.excluded_article_title_pattern and re.search(source.excluded_article_title_pattern, data['title']):
        return observe(False, 'excluded_article_title')
    declarations = []
    for script in soup.select('script[type="application/ld+json"]'):
        try:
            options = {} if require_body_layout else {'object_pairs_hook': _diagnostic_unique_object}
            declarations.extend(node for node in _objects(json.loads(script.get_text(), **options))
                                if node.get('@type') == 'NewsArticle'
                                and normalize_url(node.get('url', '')) == normalize_url(context.requested_url))
        except (NewsError, ValueError, TypeError, RecursionError):
            return observe(False, 'schema_json_not_verified')
    if not declarations:
        return observe(False, 'matching_schema_not_found')
    if not require_body_layout and len(declarations) != 1:
        return observe(False, 'matching_schema_not_found')
    for node in declarations:
        try:
            clock = datetime.fromisoformat(node.get('datePublished', '').replace('Z', '+00:00'))
            if (clock.tzinfo is None or clock.timestamp() != data['publishAt'] or node.get('headline') != data['title']
                    or str(node.get('isAccessibleForFree', 'true')).lower() == 'false'
                    or not require_body_layout and not (node.get('isAccessibleForFree', True) is True
                        or type(node.get('isAccessibleForFree')) is str and node['isAccessibleForFree'] == 'true')):
                return observe(False, 'schema_clock_or_title_or_access_not_verified')
        except (ValueError, TypeError, AttributeError, OverflowError):
            return observe(False, 'schema_clock_or_title_or_access_not_verified')
    return observe(True, 'canonical_current_public_scope_proved' if require_body_layout else
                   'canonical_current_public_identity_without_body_layout_proved' if require_heading_identity else
                   'canonical_current_public_metadata_scope_proved')


def _diagnostic_heading_identity(heading):
    """Private metadata witness, not body visibility or permission inference."""
    nodes = [heading, *heading.parents, *heading.find_all(True)]
    for node in nodes:
        if node.name in {'script', 'style', 'template', 'noscript', 'nav', 'footer', 'aside', 'dialog'}:
            return False
        if ('hidden' in node.attrs or 'inert' in node.attrs
                or str(node.get('aria-hidden', '')).lower() == 'true'):
            return False
        styles = re.sub(r'\s+|/\*.*?\*/', '', str(node.get('style', '')).lower())
        if re.search(r'(?:^|;)(?:display:none|visibility:(?:hidden|collapse)|content-visibility:hidden|opacity:0(?:[.]0+)?)(?:!important)?(?:;|$)', styles):
            return False
        classes = node.get('class', [])
        classes = classes.split() if isinstance(classes, str) else classes
        if {'hidden', 'd-none', 'invisible', 'sr-only', 'visually-hidden', 'screen-reader-only'}.intersection(classes):
            return False
    return True


def _diagnostic_unique_object(pairs):
    value = {}
    for key, item in pairs:
        if key in value:
            raise ValueError('ambiguous_diagnostic_schema_key')
        value[key] = item
    return value


def _licensed_globalvoices_scope(context, source):
    """Prove current article rights independently of its failed body parser."""
    if (source.provider != 'globalvoices' or context.response.status != 200
            or normalize_url(context.response.url) != normalize_url(context.requested_url)
            or context.response.headers.get('content-type', '').split(';')[0].strip().lower()
            not in {'text/html', 'application/xhtml+xml'}):
        return False
    from downloader.news_fulltext_policy import _objects, public_article_document
    from downloader.news_fulltext_publishers import _globalvoices_visible_article_identity

    # Reuse the canonical challenge/paywall gate. A page-wide footer license,
    # hidden byline, naive clock or another article cannot grant raw capture.
    soup = public_article_document(context.response.body, source, page_url=context.requested_url)
    if _globalvoices_visible_article_identity(soup, source, page_url=context.requested_url) is None:
        return False
    for script in soup.select('script[type="application/ld+json"]'):
        for node in _objects(json.loads(script.get_text())):
            if str(node.get('isAccessibleForFree', 'true')).lower() == 'false':
                return False
    return True


def _index_failure_context(queue, task, error):
    """Same source, request and strict failure; no I/O, grant or quota change."""
    context = getattr(error, 'failure_response', None)
    if error.code != 'cnyes_index_unverified_metadata' or not isinstance(context, FailureResponse):
        return None
    from downloader.news_fulltext_cnyes_index import is_index_task, parse_index, validate_request
    source = queue.registry.require(task['url'])
    if (not is_index_task(task) or not source.allowed or context.provider != source.provider
            or context.requested_url != task['url'] or context.response.url != task['url']
            or context.source_fingerprint != source.cnyes_index_data_fingerprint
            or context.response.status != 200 or len(context.response.body) > 8 << 20):
        return None
    validate_request(task['url'], source)
    observed = datetime.fromisoformat(context.received_at_utc)
    if observed.utcoffset() is None:
        return None
    try:
        parse_index(context.response, source)
    except NewsError as proved:
        if (proved.code == error.code and getattr(proved, 'cnyes_index_failure_details', None)
                == getattr(error, 'cnyes_index_failure_details', None)):
            return source, context, observed, proved.cnyes_index_failure_details, proved.cnyes_index_metadata_raw_allowed is True
    return None


def index_failure_metadata(queue, task, error):
    """Bounded typed facts in the EXISTING failed-request witness, not raw.

    Raw reservations may legitimately be spent. Error-field reasons must not
    disappear with them: this small metadata leaf accompanies the canonical
    source failure update without adding an archive, URL key or raw allowance.
    """
    try:
        proved = _index_failure_context(queue, task, error)
        if proved is None:
            return None
        source, context, observed, detail, raw_allowed = proved
        from downloader.news_fulltext_cnyes_index import FAILURE_DIAGNOSTIC_CONTRACT_VERSION
        metadata = {
            'contract_version': FAILURE_DIAGNOSTIC_CONTRACT_VERSION,
            'response_sha256': hashlib.sha256(context.response.body).hexdigest(),
            'response_bytes': len(context.response.body),
            'validation_observed_at_utc': context.received_at_utc,
            'authorization_fingerprint': source.authorization_fingerprint,
            'failure_details': detail, 'failure_details_complete': True,
            'raw_response_retained': False, 'accepted_body_count': 0,
            'permission_granted': False, 'publish': False, 'remote_training_export': False,
            'clock_basis': 'local validator-failure observation; not HTTP-arrival or publication time',
        }
        if len(json.dumps(metadata, separators=(',', ':'), allow_nan=False).encode()) > 4096:
            # The JSON decoder already bounds integer parsing, but malformed
            # source scalars may still be large. Keep types/reasons and exact
            # value hashes instead of unbounded numbers or invented values.
            detail = json.loads(json.dumps(detail))
            for fields in detail['checked_fields'].values():
                if 'value' in fields:
                    fields['value_sha256'] = hashlib.sha256(json.dumps(fields.pop('value')).encode()).hexdigest()
                    fields['value_omitted_for_bound'] = True
            detail['news_id_sha256'] = hashlib.sha256(str(detail.pop('news_id')).encode()).hexdigest()
            metadata['failure_details'] = detail
            metadata['failure_details_complete'] = False
        if len(json.dumps(metadata, separators=(',', ':'), allow_nan=False).encode()) > 4096:
            queue.metrics.count(task['provider'], 'public_index_field_metadata_oversize')
            return None
        queue.metrics.count(source.provider, 'public_index_field_metadata_observations')
        return metadata
    except Exception:
        queue.metrics.count(task['provider'], 'public_index_field_metadata_errors')
        return None  # An invalid diagnostic cannot undo the original source failure.


def retain_index_failure_response(queue, task, error):
    """A rejected public index is metadata, not a body or permission grant.

    Reuse the original provider-general daily/hourly reservations unchanged.
    This failure-only queue-owner sink receives normal source bytes, validates
    the exact scoped cause again and preserves it privately. Unknown fields
    permit only bounded field diagnostics, never their values or raw bodies.
    """
    context = getattr(error, 'failure_response', None)
    if error.code != 'cnyes_index_unverified_metadata' or not isinstance(context, FailureResponse):
        return
    try:
        from downloader.news_fulltext_cnyes_index import FAILURE_DIAGNOSTIC_CONTRACT_VERSION as INDEX_DIAGNOSTIC_VERSION
        proved = _index_failure_context(queue, task, error)
        if proved is None:
            return
        source, context, observed, detail, raw_allowed = proved
        budget = _reserve_evidence(queue, source.provider, observed, None)
        if budget is None:
            return
        utc = observed.astimezone(timezone.utc)
        stamp = utc.strftime('%Y%m%dT%H%M%S%fZ') + '-' + uuid.uuid4().hex[:8]
        target = queue.root / 'diagnostics' / utc.strftime('%Y%m%d') / source.provider / 'index' / (stamp + '.parquet')
        identity = {
            'contract_version': INDEX_DIAGNOSTIC_VERSION, 'provider': source.provider,
            'budget_contract_version': FAILURE_DIAGNOSTIC_BUDGET_CONTRACT_VERSION, **budget,
            'request_url': task['url'], 'final_url': context.response.url,
            'request_url_sha256': hashlib.sha256(task['url'].encode()).hexdigest(),
            'response_sha256': hashlib.sha256(context.response.body).hexdigest(),
            'response_bytes': len(context.response.body), 'status': 200, 'error_code': error.code,
            'captured_at_utc': context.received_at_utc,
            'clock_basis': 'local validator-failure observation; not HTTP-arrival or publication time',
            'index_fingerprint': source.cnyes_index_data_fingerprint,
            'authorization_fingerprint': source.authorization_fingerprint,
            'failure_details_json': json.dumps(detail, sort_keys=True, allow_nan=False),
            'headers_json': json.dumps({k:v[:2048] for k,v in context.response.headers.items()
                                       if k in {'content-type','content-length','content-encoding','server'}}),
            'diagnostic_only': True, 'metadata_only': True, 'accepted_as_article': False,
            'accepted_body_count': 0, 'publish': False, 'remote_training_export': False,
            'raw_response_retained': raw_allowed,
            'retention_basis': ('registered_metadata_endpoint_and_all_reviewed_row_fields'
                                if raw_allowed else 'metadata_field_diagnostics_only_unreviewed_fields_present'),
        }
        target.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        atomic_write_parquet(target, table_from_records([{**identity,
            'raw_response_body': context.response.body if raw_allowed else None}]),
            compression='zstd', compression_level=9, durable=True)
        target.chmod(0o600)
        receipt = target.with_suffix('.receipt.json')
        atomic_write_json(receipt, {**identity, 'path': str(target.relative_to(queue.root)),
                                   'sha256': sha256_file(target)}, durable=True)
        receipt.chmod(0o600)
        queue.metrics.count(source.provider, 'public_index_failure_diagnostics_saved')
        queue.metrics.count(source.provider, 'public_index_failure_raw_diagnostics_saved' if raw_allowed
                            else 'public_index_failure_metadata_diagnostics_saved')
    except Exception:
        # The authoritative error and source due time were committed first.
        # Storage failures consume their original reservation, not new requests
        # or refilled budgets; other providers keep working normally.
        queue.metrics.count(task['provider'], 'public_index_failure_diagnostic_write_errors')


def retain_failure_response(queue, row, error):
    """Best-effort diagnostics; the original failure is persisted first."""
    context = getattr(error, 'failure_response', None)
    if error.code in {'article_identity_not_verified', *VALIDATION_METADATA_CODES} and isinstance(context, FailureResponse):
        retain_identity_metadata(queue, row, error, context)
        return
    if error.code != 'body_not_found' or not isinstance(context, FailureResponse):
        return
    provider = row['provider']
    focus, reservation = None, None
    preproved_scope = None
    try:
        source = queue.registry.require(row['url'])
        if (not source.allowed or not re.fullmatch(r'[a-z0-9_]+', provider) or context.requested_url != row['url']
                or context.provider != provider or source.provider != provider
                or source.data_fingerprint != context.source_fingerprint
                or queue.registry.require(context.response.url).provider != provider
                or context.response.status != 200 or len(context.response.body) > 8 << 20):
            return
        clock = datetime.fromisoformat(context.received_at_utc)
        if clock.tzinfo is None:
            return
        day = clock.astimezone(timezone.utc).strftime('%Y%m%d')
        failure_stage = _native_stage(provider, error)
        focus = getattr(queue, 'failure_diagnostic_focus', None)
        if focus is not None and row['url'] in focus.targets:
            # Explicit finite missing-response scope; no extra request, quota
            # reset or rights grant. Duplicate/unknown writes return before
            # expensive scope parsing and do not spend the general budget.
            if not focus.needs_response(row['url']):
                return
            if focus.metadata_upgrade_pending(row['url']):
                # A prior metadata-only pair cannot be reset or overwritten.
                # Only a new, already-received NORMAL response with positive
                # free/current/unique-schema identity may spend attempt two.
                # No reservation is spent on another inconclusive proof.
                preproved_scope = {}
                try:
                    proved = _public_cnyes_scope(context, source, preproved_scope,
                        require_body_layout=False, require_heading_identity=False)
                except (NewsError, ValueError, TypeError, AttributeError, OverflowError, RecursionError):
                    proved = False
                if not proved:
                    queue.metrics.count(provider, 'failure_metadata_identity_upgrade_scope_unproved')
                    reason = preproved_scope.get('first_failed_check')
                    if reason in CNYES_RAW_SCOPE_REASONS:
                        queue.metrics.count(provider, 'failure_metadata_identity_upgrade_unproved_' + reason)
                    return
            stamp = clock.astimezone(timezone.utc).strftime('%Y%m%dT%H%M%S%fZ') + '-' + uuid.uuid4().hex[:8]
            response_sha = hashlib.sha256(context.response.body).hexdigest()
            target = queue.root / 'diagnostics' / day / provider / 'focus' / focus.id / (stamp + '.parquet')
            reservation = focus.reserve(row['url'], len(context.response.body), response_sha,
                str(target.with_suffix('.receipt.json').relative_to(queue.root)),
                metadata_identity_proved=preproved_scope is not None)
            budget = {key: value for key, value in reservation.items() if not key.startswith('_')} if reservation else None
        else:
            focus = None
            budget = _reserve_evidence(queue, provider, clock, failure_stage)
            if budget is None:
                return
            stamp = clock.astimezone(timezone.utc).strftime('%Y%m%dT%H%M%S%fZ') + '-' + uuid.uuid4().hex[:8]
            target = queue.root / 'diagnostics' / day / provider / (stamp + '.parquet')
        if budget is None:
            return
        scope_observation = preproved_scope if preproved_scope is not None else {}
        try:
            public_scope = preproved_scope is not None or _public_cnyes_scope(context, source, scope_observation)
            if not public_scope and scope_observation.get('first_failed_check') == 'visible_article_layout_not_verified':
                # A failed body selector cannot be a prerequisite for diagnosing
                # that selector. This independently proves free public metadata
                # identity, retaining received bytes privately, never accepting
                # a body, requesting again or changing source authorization.
                public_scope = _public_cnyes_scope(context, source, scope_observation, require_body_layout=False)
            if not public_scope and scope_observation.get('first_failed_check') == 'diagnostic_heading_identity_not_verified':
                # Rendering failure is not a prerequisite for PRIVATE source
                # diagnosis. Require independent positive canonical, current
                # ID/payment0/title/URL and unique schema/aware-clock proof.
                public_scope = _public_cnyes_scope(context, source, scope_observation,
                    require_body_layout=False, require_heading_identity=False)
            retention_basis = ('matching_current_public_payment0_identity_and_aware_clock'
                               if public_scope else
                               'matching_current_native_article_CC_license_authors_and_aware_clock'
                               if _licensed_globalvoices_scope(context, source) else
                               'matching_native_taiwanhot_identity_author_and_minute_clock_private_diagnostic_only'
                               if _public_taiwanhot_scope(context, source) else
                               'metadata_only_no_body_scope_inferred')
            if public_scope and scope_observation.get('independent_identity_only'):
                retention_basis = 'layout_independent_current_public_payment0_identity_and_aware_clock_diagnostic_only'
            if public_scope and scope_observation.get('contract_version') == PUBLIC_METADATA_RAW_SCOPE_CONTRACT_VERSION:
                retention_basis = 'canonical_current_public_metadata_identity_for_private_diagnostic_only'
            raw_allowed = retention_basis != 'metadata_only_no_body_scope_inferred'
        except (NewsError, ValueError, TypeError, AttributeError, OverflowError, RecursionError):
            raw_allowed = False  # Broken evidence permits metadata only.
            retention_basis = 'metadata_only_no_body_scope_inferred'
            _observe_raw_scope(scope_observation, False, 'scope_check_exception')
        raw = context.response.body
        response_sha = response_sha if focus is not None else hashlib.sha256(raw).hexdigest()
        requested_display, final_display = _display_url(row['url']), _display_url(context.response.url)
        if raw_allowed and provider == 'cnyes':
            from downloader.news_fulltext_publishers import cnyes_supported_native_alias_view
            if cnyes_supported_native_alias_view(urlsplit(row['url'])):
                # Keep only the proven, non-secret native variant so private
                # source replay uses the actual received representation. All
                # unverified/other-provider queries remain redacted + hashed.
                requested_display = normalize_url(row['url'])
                final_display = normalize_url(context.response.url)
        identity = {
            'contract_version': (METADATA_UPGRADE_RECEIPT_CONTRACT_VERSION
                if budget.get('failure_diagnostic_metadata_identity_upgrade_contract_version') else
                FOCUS_RECEIPT_CONTRACT_VERSION if focus is not None else FAILURE_DIAGNOSTIC_CONTRACT_VERSION),
            'provider': provider,
            'budget_contract_version': FAILURE_DIAGNOSTIC_BUDGET_CONTRACT_VERSION,
            **budget,
            'request_url': requested_display, 'final_url': final_display,
            'request_url_sha256': hashlib.sha256(row['url'].encode()).hexdigest(),
            'final_url_sha256': hashlib.sha256(context.response.url.encode()).hexdigest(),
            'response_sha256': response_sha, 'response_bytes': len(raw), 'status': context.response.status,
            'error_code': error.code, 'captured_at_utc': context.received_at_utc,
            'native_body_failure_observation_contract_version': CNYES_NATIVE_BODY_FAILURE_OBSERVATION_CONTRACT_VERSION if failure_stage else None,
            'native_body_failure_stage': failure_stage,
            'data_fingerprint': source.data_fingerprint, 'authorization_json': json.dumps(source.authorization, ensure_ascii=False),
            'headers_json': json.dumps({k: v[:2048] for k, v in context.response.headers.items()
                                       if k in {'content-type', 'content-length', 'content-encoding', 'server'}}),
            'diagnostic_only': True, 'accepted_as_article': False, 'publish': False,
            'remote_training_export': False, 'raw_response_retained': raw_allowed,
            'retention_basis': retention_basis,
            **({'raw_scope_observation': scope_observation} if provider == 'cnyes' else {}),
        }
        target.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        atomic_write_parquet(target, table_from_records([{**identity, 'raw_response_body': raw if raw_allowed else None}]),
                             compression='zstd', compression_level=9, durable=True)
        atomic_write_json(target.with_suffix('.receipt.json'), {**identity, 'path': str(target.relative_to(queue.root)),
                                                              'sha256': sha256_file(target)}, durable=True)
        if focus is not None:
            focus.finish(reservation, 'raw_saved' if raw_allowed else 'metadata_saved')
        queue.metrics.count(provider, 'failure_response_diagnostics_saved')
        queue.metrics.count(provider, 'failure_diagnostic_' + budget['budget_period_kind'] + '_saved')
        queue.metrics.count(provider, 'failure_raw_diagnostics_saved' if raw_allowed else 'failure_metadata_diagnostics_saved')
    except Exception:
        # A diagnostic sink is subordinate to the source outcome. Preserve
        # even an unexpected storage/Arrow/SQLite error as a counter, without
        # turning an already recorded failure into success or stopping peers.
        queue.metrics.count(provider, 'failure_diagnostic_write_errors')
        if focus is not None and reservation is not None:
            # A written durable pair may outlive its SQLite finish. Preserve
            # it as unknown for startup readback, never overwrite or retry it.
            if not target.exists() and not target.with_suffix('.receipt.json').exists():
                try:
                    focus.finish(reservation, 'write_failed')
                except Exception:
                    queue.metrics.count(provider, 'failure_focus_outcome_write_errors')
