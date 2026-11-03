"""Bounded, read-only projection of independently checked corpus quality evidence.

The audit and its readback are private. Only reconciled counts and clocks leave
this module; source URLs, article IDs, filesystem paths and text never do.
This is positive evidence for a pinned corpus, not a corpus-wide quality test.
"""
from __future__ import annotations

from collections import Counter
from datetime import UTC, datetime, timedelta
import hashlib
import json
import os
from pathlib import Path
import re
import stat
from typing import Any

from stockagent.data.news_dataset_sources import headline_metadata_only_reason
from stockagent.live.dashboard_updates import metadata_signature, read_json_snapshot


REFERENCE = Path('artifacts/data_quality/news_corpus_quality/latest.json')
HEADLINE_REFERENCE = Path('artifacts/data_quality/news_corpus_quality/cmnews_tag_latest.json')
SORT_DAY_REFERENCE = Path('artifacts/data_quality/news_corpus_quality/sort_day_latest.json')
ARTIFACTS = Path('artifacts/data_quality')
MAX_REFERENCE_BYTES = 16 * 1024
MAX_PROOF_BYTES = 4 * 1024 * 1024
MAX_MANIFEST_BYTES = 16 * 1024 * 1024
_SHA = re.compile(r'^[0-9a-f]{64}$')
_PROVIDER = re.compile(r'^(?:[a-z][a-z0-9_]{0,63}|unregistered:[a-z0-9.-]{1,253})$')
_ORIGINS = frozenset(('owner_admitted', 'private_staged'))
_MAX_COUNT = 2**53 - 1


class QualityEvidenceError(ValueError):
    """An evidence boundary failed; error details are not a public DTO."""


def _unique_pairs(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise QualityEvidenceError('duplicate_json_key')
        result[key] = value
    return result


def _json(raw):
    try:
        value = json.loads(raw, object_pairs_hook=_unique_pairs)
    except (UnicodeError, ValueError, RecursionError) as exc:
        raise QualityEvidenceError('invalid_json') from exc
    if not isinstance(value, dict):
        raise QualityEvidenceError('json_object_required')
    return value


def _read(path: Path, limit: int):
    try:
        before = path.stat()
        signature = metadata_signature(before)
        if not stat.S_ISREG(before.st_mode) or not 0 < before.st_size <= limit:
            raise QualityEvidenceError('file_not_bounded_regular')
        with path.open('rb') as stream:
            if metadata_signature(os.fstat(stream.fileno())) != signature:
                raise QualityEvidenceError('file_changed_before_read')
            content = stream.read(limit + 1)
            finished = metadata_signature(os.fstat(stream.fileno()))
        if (len(content) != before.st_size or len(content) > limit or finished != signature
                or metadata_signature(path.stat()) != signature):
            raise QualityEvidenceError('file_changed_during_read')
        return content, signature
    except OSError as exc:
        raise QualityEvidenceError('file_unavailable') from exc


def _artifact(repo_root: Path, value: Any) -> Path:
    if not isinstance(value, str) or not value or len(value) > 1024:
        raise QualityEvidenceError('invalid_artifact_path')
    relative = Path(value)
    if relative.is_absolute() or '..' in relative.parts or relative in (REFERENCE, HEADLINE_REFERENCE, SORT_DAY_REFERENCE):
        raise QualityEvidenceError('invalid_artifact_path')
    try:
        root = (repo_root / ARTIFACTS).resolve(strict=True)
        path = (repo_root / relative).resolve(strict=True)
    except (OSError, RuntimeError, ValueError) as exc:
        raise QualityEvidenceError('artifact_unavailable') from exc
    if not path.is_relative_to(root) or path == root:
        raise QualityEvidenceError('artifact_outside_quality_root')
    return path


def _artifact_revision_path(repo_root: Path, value: Any) -> Path:
    """Watch safe missing paths as well, so restoring evidence invalidates cache."""
    if not isinstance(value, str) or not value or len(value) > 1024:
        raise QualityEvidenceError('invalid_artifact_path')
    relative = Path(value)
    if relative.is_absolute() or '..' in relative.parts or relative in (REFERENCE, HEADLINE_REFERENCE, SORT_DAY_REFERENCE):
        raise QualityEvidenceError('invalid_artifact_path')
    try:
        root = (repo_root / ARTIFACTS).resolve(strict=True)
        resolved = (repo_root / relative).resolve(strict=False)
    except (OSError, RuntimeError, ValueError) as exc:
        raise QualityEvidenceError('artifact_unavailable') from exc
    if not resolved.is_relative_to(root) or resolved == root:
        raise QualityEvidenceError('artifact_outside_quality_root')
    return repo_root / relative


def _checked_pointer(repo_root: Path, reference: Path = REFERENCE) -> Path:
    pointer = repo_root / reference
    try:
        root = (repo_root / ARTIFACTS).resolve(strict=True)
        resolved = pointer.resolve(strict=True)
    except (OSError, RuntimeError, ValueError) as exc:
        raise QualityEvidenceError('reference_unavailable') from exc
    if not resolved.is_relative_to(root):
        raise QualityEvidenceError('reference_outside_quality_root')
    return pointer


def _clock(value: Any) -> datetime:
    if not isinstance(value, str):
        raise QualityEvidenceError('invalid_observation_clock')
    try:
        parsed = datetime.fromisoformat(value.replace('Z', '+00:00'))
        if parsed.utcoffset() is None:
            raise ValueError('timezone required')
        return parsed.astimezone(UTC)
    except (ValueError, OverflowError) as exc:
        raise QualityEvidenceError('invalid_observation_clock') from exc


def _count(value: Any) -> bool:
    return type(value) is int and 0 <= value <= _MAX_COUNT


def _entries(reference: dict):
    if (not isinstance(reference, dict) or set(reference) != {'contract_version', 'audit', 'readback'}
            or type(reference.get('contract_version')) is not int or reference['contract_version'] != 1):
        raise QualityEvidenceError('invalid_reference')
    for name in ('audit', 'readback'):
        entry = reference.get(name)
        if (not isinstance(entry, dict) or set(entry) != {'path', 'sha256'}
                or not isinstance(entry.get('sha256'), str) or not _SHA.fullmatch(entry['sha256'])):
            raise QualityEvidenceError('invalid_reference_entry')


def build_reference(repo_root: Path, audit: Path, readback: Path) -> dict:
    """Hash supplied private evidence and create a bounded local reference."""
    entries = {}
    for name, value in (('audit', audit), ('readback', readback)):
        candidate = value if value.is_absolute() else repo_root / value
        try:
            relative = candidate.absolute().relative_to(repo_root.absolute())
        except ValueError as exc:
            raise QualityEvidenceError('evidence_must_be_inside_repo') from exc
        raw, _ = _read(_artifact(repo_root, relative.as_posix()), MAX_PROOF_BYTES)
        entries[name] = {'path': relative.as_posix(), 'sha256': hashlib.sha256(raw).hexdigest()}
    return {'contract_version': 1, **entries}


def build_sort_day_reference(repo_root: Path, audit: Path) -> dict:
    """Bind a historical queue-hint audit, not the training corpus."""
    candidate = audit if audit.is_absolute() else repo_root / audit
    try:
        relative = candidate.absolute().relative_to(repo_root.absolute())
    except ValueError as exc:
        raise QualityEvidenceError('evidence_must_be_inside_repo') from exc
    raw, _ = _read(_artifact(repo_root, relative.as_posix()), MAX_PROOF_BYTES)
    return {'contract_version': 1, 'audit': {'path': relative.as_posix(),
                                            'sha256': hashlib.sha256(raw).hexdigest()}}


def _sort_entry(reference: dict) -> dict:
    if (not isinstance(reference, dict) or set(reference) != {'contract_version', 'audit'}
            or type(reference.get('contract_version')) is not int or reference['contract_version'] != 1):
        raise QualityEvidenceError('invalid_sort_reference')
    entry = reference['audit']
    if (not isinstance(entry, dict) or set(entry) != {'path', 'sha256'}
            or not isinstance(entry.get('sha256'), str) or not _SHA.fullmatch(entry['sha256'])):
        raise QualityEvidenceError('invalid_sort_reference_entry')
    return entry


def inspect_sort_day_reference(repo_root: Path, reference: dict, *, now: datetime) -> dict:
    """Project a pinned historical scan; no currentness or publisher-clock claim."""
    if now.utcoffset() is None:
        raise ValueError('timezone required')
    entry = _sort_entry(reference)
    path = _artifact(repo_root, entry['path'])
    raw, signature = _read(path, MAX_PROOF_BYTES)
    if hashlib.sha256(raw).hexdigest() != entry['sha256']:
        raise QualityEvidenceError('sort_audit_hash_mismatch')
    audit = _json(raw)
    if (audit.get('contract') not in ('all_source_sort_day_quality_index_readback_v1',
                                      'news_sort_day_inventory_summary_v1')
            or type(audit.get('publisher_requests')) is not int or audit['publisher_requests'] != 0
            or audit.get('production_queue_writes') is not False):
        raise QualityEvidenceError('unsupported_sort_audit')
    if audit['contract'] == 'news_sort_day_inventory_summary_v1':
        source_hash = audit.get('source_inventory_sha256')
        if not isinstance(source_hash, str) or not _SHA.fullmatch(source_hash):
            raise QualityEvidenceError('invalid_sort_inventory_identity')
    else:
        files = audit.get('snapshot_files')
        if (audit.get('news_owner_lock_held') is not True or not isinstance(files, list) or len(files) != 3
                or not isinstance(files[0], dict) or any(file is not None and
                   (not isinstance(file, dict) or type(file.get('size')) is not int or file['size'] != 0)
                   for file in files[1:])):
            raise QualityEvidenceError('invalid_historical_sort_snapshot')
    observed = _clock(audit.get('observed_at_utc'))
    if observed > now.astimezone(UTC) + timedelta(seconds=5) or observed.year < 1900:
        raise QualityEvidenceError('invalid_sort_audit_clock')
    if audit.get('utc_comparison_day') != observed.date().isoformat():
        raise QualityEvidenceError('sort_comparison_day_mismatch')
    rows, totals = audit.get('providers'), audit.get('totals')
    fields = ('known_heads', 'present', 'invalid', 'pre_1900', 'after_snapshot_utc')
    if (not isinstance(rows, list) or len(rows) > 4096 or not isinstance(totals, dict)
            or type(audit.get('provider_keys_with_heads')) is not int
            or audit['provider_keys_with_heads'] != len(rows)):
        raise QualityEvidenceError('invalid_sort_audit_scope')
    seen, sums, flagged = set(), Counter(), []
    for row in rows:
        if (not isinstance(row, dict) or not isinstance(row.get('provider'), str)
                or not _PROVIDER.fullmatch(row['provider']) or row['provider'] in seen
                or any(not _count(row.get(field)) for field in fields)):
            raise QualityEvidenceError('invalid_sort_audit_row')
        if (row['known_heads'] == 0 or row['present'] > row['known_heads']
                or sum(row[field] for field in fields[2:]) > row['present']):
            raise QualityEvidenceError('invalid_sort_audit_counts')
        seen.add(row['provider'])
        sums.update({field: row[field] for field in fields})
        if any(row[field] for field in fields[2:]):
            flagged.append({field: row[field] for field in ('provider', *fields)})
    if (any(not _count(totals.get(field)) or totals[field] != sums[field] for field in fields)
            or audit.get('flagged_providers') != flagged):
        raise QualityEvidenceError('sort_audit_totals_mismatch')
    try:
        if metadata_signature(path.stat()) != signature:
            raise QualityEvidenceError('sort_audit_changed_after_read')
    except OSError as exc:
        raise QualityEvidenceError('sort_audit_unavailable_after_read') from exc
    return {'state': 'historical_audit', 'observed_at_utc': observed.isoformat(),
            'source_keys_with_heads': len(rows), **{field: sums[field] for field in fields},
            'flagged_sources': [{'id': row['provider'], **{field: row[field] for field in fields[2:]}}
                                for row in flagged],
            'publication_times_verified': False, 'current_queue_claimed': False}


def public_sort_day_quality_status(repo_root: Path, *, now: datetime) -> dict:
    """Absent, damaged or changing history is unknown, never a clean result."""
    pointer = repo_root / SORT_DAY_REFERENCE
    try:
        raw, signature = _read(_checked_pointer(repo_root, SORT_DAY_REFERENCE), MAX_REFERENCE_BYTES)
        result = inspect_sort_day_reference(repo_root, _json(raw), now=now)
        if metadata_signature(pointer.stat()) != signature:
            return {'state': 'unavailable'}
        return result
    except (QualityEvidenceError, OSError, TypeError, ValueError, KeyError):
        return {'state': 'unavailable'}


def reference_paths(repo_root: Path) -> list[Path]:
    """Files to include in dashboard invalidation, never paths for the client."""
    paths = []
    for pointer_name in (REFERENCE, HEADLINE_REFERENCE):
        pointer = repo_root / pointer_name
        if not pointer.exists() and not pointer.is_symlink():
            continue
        paths.append(pointer)
        try:
            reference = read_json_snapshot(_checked_pointer(repo_root, pointer_name), max_bytes=MAX_REFERENCE_BYTES)
            _entries(reference)
            paths.extend(_artifact_revision_path(repo_root, reference[name]['path']) for name in ('audit', 'readback'))
        except (QualityEvidenceError, TypeError):
            pass
    pointer = repo_root / SORT_DAY_REFERENCE
    if pointer.exists() or pointer.is_symlink():
        paths.append(pointer)
        try:
            reference = read_json_snapshot(_checked_pointer(repo_root, SORT_DAY_REFERENCE), max_bytes=MAX_REFERENCE_BYTES)
            paths.append(_artifact_revision_path(repo_root, _sort_entry(reference)['path']))
        except (QualityEvidenceError, TypeError):
            pass
    if any((repo_root / name).exists() or (repo_root / name).is_symlink()
           for name in (REFERENCE, HEADLINE_REFERENCE)):
        paths.append(repo_root / 'data_news/manifest.json')
    return paths


def _manifest_observation(repo_root: Path) -> tuple[str, tuple]:
    raw, signature = _read(repo_root / 'data_news/manifest.json', MAX_MANIFEST_BYTES)
    return hashlib.sha256(raw).hexdigest(), signature


def inspect_reference(repo_root: Path, reference: dict, *, now: datetime,
                      manifest_observation: tuple | None = None) -> dict:
    """Validate two private observations and bind them to the current manifest."""
    _entries(reference)
    if now.utcoffset() is None:
        raise ValueError('timezone required')
    audit_path = _artifact(repo_root, reference['audit']['path'])
    readback_path = _artifact(repo_root, reference['readback']['path'])
    if audit_path == readback_path:
        raise QualityEvidenceError('audit_and_readback_must_differ')
    manifest_path = repo_root / 'data_news/manifest.json'
    audit_raw, audit_signature = _read(audit_path, MAX_PROOF_BYTES)
    readback_raw, readback_signature = _read(readback_path, MAX_PROOF_BYTES)
    current_manifest_sha, manifest_signature = manifest_observation or _manifest_observation(repo_root)
    audit_hash = hashlib.sha256(audit_raw).hexdigest()
    if (audit_hash != reference['audit']['sha256']
            or hashlib.sha256(readback_raw).hexdigest() != reference['readback']['sha256']):
        raise QualityEvidenceError('evidence_hash_mismatch')
    audit, readback = _json(audit_raw), _json(readback_raw)
    if (audit.get('contract') != 'news_corpus_reviewed_body_positive_audit_v1'
            or readback.get('contract') != 'news_corpus_reviewed_versions_independent_readback_v1'
            or readback.get('audit_sha256') != audit_hash
            or audit.get('negative_matches_prove_absence') is not False
            or audit.get('all_corpus_parts_sha_verified') is not False
            or any(type(item.get(key)) is not int or item[key] != 0
                   for item in (audit, readback)
                   for key in ('publisher_requests', 'production_queue_writes', 'production_corpus_writes'))):
        raise QualityEvidenceError('unsupported_evidence_contract')
    first, second = _clock(audit.get('observed_at_utc')), _clock(readback.get('observed_at_utc'))
    if second < first or second > now.astimezone(UTC) + timedelta(seconds=5):
        raise QualityEvidenceError('invalid_evidence_clock_order')
    manifest_sha = audit.get('corpus_manifest_sha256')
    generation = audit.get('corpus_generation')
    source_snapshot = audit.get('source_snapshot')
    if (not isinstance(manifest_sha, str) or not _SHA.fullmatch(manifest_sha)
            or not isinstance(generation, str) or not generation.startswith('versions/')
            or readback.get('corpus_manifest_sha256') != manifest_sha
            or readback.get('corpus_generation') != generation
            or not isinstance(source_snapshot, list) or len(source_snapshot) != 3
            or source_snapshot[1:] != [None, None]
            or not isinstance(source_snapshot[0], list) or len(source_snapshot[0]) != 5
            or any(type(value) is not int or value < 0 for value in source_snapshot[0])
            or source_snapshot != readback.get('source_snapshot')):
        raise QualityEvidenceError('evidence_identity_mismatch')
    count = audit.get('confirmed_exact_versions')
    targets, unconfirmed = audit.get('targets'), audit.get('unconfirmed_targets')
    matches, groups = audit.get('matches'), readback.get('by_provider_and_review_origin')
    positive_parts = audit.get('positive_part_checksums_verified')
    if (not all(_count(value) for value in (count, targets, unconfirmed, positive_parts))
            or count + unconfirmed != targets or not isinstance(matches, list) or len(matches) != count
            or type(readback.get('confirmed_exact_fulltext_versions')) is not int
            or readback['confirmed_exact_fulltext_versions'] != count
            or type(readback.get('positive_corpus_parts')) is not int
            or readback['positive_corpus_parts'] != positive_parts
            or not isinstance(groups, list)):
        raise QualityEvidenceError('evidence_counts_mismatch')
    actual, identities, parts = Counter(), set(), {}
    for match in matches:
        if not isinstance(match, dict):
            raise QualityEvidenceError('invalid_positive_match')
        provider, origin, article = match.get('provider'), match.get('origin'), match.get('article_id')
        part, part_hash = match.get('corpus_part'), match.get('corpus_part_sha256')
        if (not isinstance(provider, str) or not _PROVIDER.fullmatch(provider) or origin not in _ORIGINS
                or not isinstance(article, str) or not _SHA.fullmatch(article)
                or not isinstance(part, str) or not part or not isinstance(part_hash, str)
                or not _SHA.fullmatch(part_hash)):
            raise QualityEvidenceError('invalid_positive_match')
        if article in identities:
            raise QualityEvidenceError('duplicate_positive_match')
        identities.add(article)
        if part in parts and parts[part] != part_hash:
            raise QualityEvidenceError('conflicting_positive_part_hash')
        parts[part] = part_hash
        actual[(provider, origin)] += 1
    independent = Counter()
    for group in groups:
        if (not isinstance(group, dict) or not isinstance(group.get('provider'), str)
                or not _PROVIDER.fullmatch(group['provider']) or group.get('origin') not in _ORIGINS
                or not _count(group.get('count')) or group['count'] == 0):
            raise QualityEvidenceError('invalid_independent_group')
        key = (group['provider'], group['origin'])
        if key in independent:
            raise QualityEvidenceError('duplicate_independent_group')
        independent[key] = group['count']
    by_origin = Counter({origin: sum(n for (_, observed), n in actual.items() if observed == origin)
                         for origin in _ORIGINS})
    by_provider = Counter()
    for (provider, _), n in actual.items():
        by_provider[provider] += n
    observed_origins, observed_providers = audit.get('confirmed_by_origin'), audit.get('confirmed_by_provider')
    if (actual != independent or len(parts) != positive_parts
            or not isinstance(observed_origins, dict) or not isinstance(observed_providers, dict)
            or observed_origins != {k: v for k, v in by_origin.items() if v}
            or observed_providers != dict(by_provider)):
        raise QualityEvidenceError('positive_groups_mismatch')
    try:
        if any(metadata_signature(path.stat()) != signature for path, signature in (
                (audit_path, audit_signature), (readback_path, readback_signature),
                (manifest_path, manifest_signature))):
            raise QualityEvidenceError('evidence_changed_after_read')
    except OSError as exc:
        raise QualityEvidenceError('evidence_unavailable_after_read') from exc
    if current_manifest_sha != manifest_sha:
        return {'state': 'superseded'}
    return {'state': 'known_issues' if count else 'no_positive_in_scope',
            'observed_at_utc': second.isoformat(), 'known_affected_versions': count,
            'owner_admitted_versions': by_origin['owner_admitted'],
            'private_staged_versions': by_origin['private_staged'],
            'providers': [{'id': provider, 'versions': n} for provider, n in sorted(by_provider.items())],
            'unconfirmed_targets': unconfirmed, 'all_corpus_parts_verified': False,
            'all_other_versions_cleared': False}


def inspect_headline_reference(repo_root: Path, reference: dict, *, now: datetime,
                               manifest_observation: tuple | None = None) -> dict:
    """Validate the tag-route headline audit without claiming page identity."""
    _entries(reference)
    if now.utcoffset() is None:
        raise ValueError('timezone required')
    audit_path = _artifact(repo_root, reference['audit']['path'])
    readback_path = _artifact(repo_root, reference['readback']['path'])
    if audit_path == readback_path:
        raise QualityEvidenceError('audit_and_readback_must_differ')
    audit_raw, audit_signature = _read(audit_path, MAX_PROOF_BYTES)
    readback_raw, readback_signature = _read(readback_path, MAX_PROOF_BYTES)
    audit_hash = hashlib.sha256(audit_raw).hexdigest()
    if (audit_hash != reference['audit']['sha256']
            or hashlib.sha256(readback_raw).hexdigest() != reference['readback']['sha256']):
        raise QualityEvidenceError('evidence_hash_mismatch')
    audit, readback = _json(audit_raw), _json(readback_raw)
    if (audit.get('contract') != 'cmnews_tag_corpus_projection_v1'
            or readback.get('contract') != 'cmnews_tag_corpus_source_readback_v1'
            or readback.get('input_report_sha256') != audit_hash
            or any(item.get(key) is not expected for item, key, expected in (
                (audit, 'source_identity_or_page_content_verified', False),
                (audit, 'body_columns_read', False),
                (audit, 'part_sha256_reverified', False),
                (audit, 'part_stat_identity_not_a_filesystem_snapshot', True),
                (readback, 'source_head_state_is_current_snapshot_not_historical_corpus_state', True),
                (readback, 'source_page_identity_or_content_verified', False),
                (readback, 'snapshot_checks_are_not_global_filesystem_snapshot', True)))
            or any(type(item.get(key)) is not int or item[key] != 0
                   for item in (audit, readback)
                   for key in ('publisher_requests', 'production_queue_writes', 'corpus_writes'))):
        raise QualityEvidenceError('unsupported_evidence_contract')
    first, second = _clock(audit.get('observed_at_utc')), _clock(readback.get('observed_at_utc'))
    if second < first or second > now.astimezone(UTC) + timedelta(seconds=5):
        raise QualityEvidenceError('invalid_evidence_clock_order')
    manifest_sha = audit.get('manifest_sha256')
    generation, identity, snapshot = audit.get('generation'), audit.get('manifest_identity'), readback.get('snapshot')
    if (not isinstance(manifest_sha, str) or not _SHA.fullmatch(manifest_sha)
            or readback.get('corpus_manifest_sha256') != manifest_sha
            or not isinstance(generation, str) or not generation.startswith('versions/')
            or not isinstance(identity, list) or len(identity) != 5
            or any(type(value) is not int or value < 0 for value in identity)
            or not isinstance(snapshot, dict) or snapshot.get('manifest') != identity):
        raise QualityEvidenceError('evidence_identity_mismatch')
    matches, counts, facts = audit.get('matches'), audit.get('counts'), readback.get('source_head_facts')
    if (not isinstance(matches, list) or not isinstance(counts, dict) or not isinstance(facts, dict)
            or not _count(audit.get('part_count'))
            or not all(_count(counts.get(key)) for key in
                       ('headline_rows_examined', 'fulltext_rows_pruned', 'fulltext_rows_examined'))
            or counts['fulltext_rows_examined'] != 0
            or len(matches) > counts['headline_rows_examined']
            or bool(matches) and not audit['part_count']
            or type(readback.get('direct_corpus_url_count')) is not int
            or readback['direct_corpus_url_count'] != len(matches)):
        raise QualityEvidenceError('evidence_counts_mismatch')
    urls = set()
    for match in matches:
        if (not isinstance(match, dict) or match.get('kind') != 'cmnews_tag_literal'
                or not isinstance(match.get('url'), str)
                or headline_metadata_only_reason(match['url']) != 'cmnews_tag_route_unverified'
                or match['url'] in urls):
            raise QualityEvidenceError('invalid_headline_match')
        urls.add(match['url'])
    count = len(matches)
    if audit.get('matches_by_kind') != ({'cmnews_tag_literal': count} if count else {}):
        raise QualityEvidenceError('headline_groups_mismatch')
    expected_facts = ('found_in_current_source', 'current_provider:cmnews',
                      'current_state:unsupported_article_path', 'current_body_pointer_absent',
                      'current_title_present', 'current_title_contains_collection_phrase')
    if any(type(facts.get(key)) is not int or facts[key] != count for key in expected_facts):
        raise QualityEvidenceError('headline_source_readback_mismatch')
    manifest_path = repo_root / 'data_news/manifest.json'
    current_manifest_sha, manifest_signature = manifest_observation or _manifest_observation(repo_root)
    try:
        if any(metadata_signature(path.stat()) != signature for path, signature in (
                (audit_path, audit_signature), (readback_path, readback_signature),
                (manifest_path, manifest_signature))):
            raise QualityEvidenceError('evidence_changed_after_read')
    except OSError as exc:
        raise QualityEvidenceError('evidence_unavailable_after_read') from exc
    if current_manifest_sha != manifest_sha:
        return {'state': 'superseded'}
    return {'state': 'tag_headlines_detected' if count else 'no_tag_headlines_in_scope',
            'observed_at_utc': second.isoformat(), 'tag_headline_rows': count,
            'headline_rows_examined': counts['headline_rows_examined'],
            'current_source_heads_confirmed': count, 'source_page_content_verified': False,
            'source_heads_are_current_snapshot': True, 'corpus_parts_sha_reverified': False,
            'all_other_headlines_cleared': False}


def _public_quality_status(repo_root: Path, *, now: datetime, pointer_name: Path, inspector,
                           manifest_observation: tuple | None = None) -> dict:
    pointer = repo_root / pointer_name
    if not pointer.exists() and not pointer.is_symlink():
        return {'state': 'unavailable'}
    try:
        raw, signature = _read(_checked_pointer(repo_root, pointer_name), MAX_REFERENCE_BYTES)
        result = inspector(repo_root, _json(raw), now=now, manifest_observation=manifest_observation)
        if metadata_signature(pointer.stat()) != signature:
            return {'state': 'unavailable'}
        return result
    except (QualityEvidenceError, OSError, TypeError, ValueError, KeyError):
        return {'state': 'unavailable'}


def public_quality_status(repo_root: Path, *, now: datetime,
                          manifest_observation: tuple | None = None) -> dict:
    """A malformed, absent or changing audit is unknown, never a clean corpus."""
    return _public_quality_status(repo_root, now=now, pointer_name=REFERENCE,
                                  inspector=inspect_reference, manifest_observation=manifest_observation)


def public_headline_quality_status(repo_root: Path, *, now: datetime,
                                   manifest_observation: tuple | None = None) -> dict:
    return _public_quality_status(repo_root, now=now, pointer_name=HEADLINE_REFERENCE,
                                  inspector=inspect_headline_reference, manifest_observation=manifest_observation)


def public_corpus_quality_statuses(repo_root: Path, *, now: datetime) -> tuple[dict, dict]:
    """Share one pinned manifest read and reject a cross-generation projection."""
    if not any((repo_root / name).exists() or (repo_root / name).is_symlink()
               for name in (REFERENCE, HEADLINE_REFERENCE)):
        return {'state': 'unavailable'}, {'state': 'unavailable'}
    try:
        observation = _manifest_observation(repo_root)
        body = public_quality_status(repo_root, now=now, manifest_observation=observation)
        headlines = public_headline_quality_status(repo_root, now=now, manifest_observation=observation)
        if metadata_signature((repo_root / 'data_news/manifest.json').stat()) != observation[1]:
            return {'state': 'unavailable'}, {'state': 'unavailable'}
        return body, headlines
    except (QualityEvidenceError, OSError, TypeError, ValueError):
        return {'state': 'unavailable'}, {'state': 'unavailable'}
