#!/usr/bin/env python3
"""Drain bounded fixed E or D-source repacks through the original owner."""
from __future__ import annotations

import fcntl
import hashlib
import json
import math
import os
from pathlib import Path
import re
import stat
import subprocess
import sys
import threading
import time

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from stockagent.data_sync import semantic_link_promotion, source_repack_bloat, staged_head_capacity_relief, stream_repack, workspace_volume
from stockagent.data_sync.cold_primary import _check_d_primary_mount
from stockagent.data_sync.dataset_migration import private_history_root
from stockagent.data_sync.dataset_version_layout import version_policy
from stockagent.data_sync.desync_snapshots import SnapshotError, atomic_write_json
from stockagent.data_sync.packed_snapshots import resolve_latest_packed, resolve_packed_snapshot_id
from scripts.publish_data_releases import DEFAULT_CATALOG, _load_catalog, _source_path

CONFIG = ROOT/'configs/data_sync/source_local_repack_queue.json'
GENERATION_HOLDS = ROOT/'configs/data_sync/source_local_repack_generation_holds.json'
CONTRACT = 'stockagent.source_local_repack_queue.v1'
DISCOVERY_CONTRACT = CONTRACT+'.discovery-v7'


def missing_cold_inventory(stderr: str) -> bool:
    """Recognize a missing content-addressed D inventory, not an arbitrary I/O error."""
    match = re.search(r"FileNotFoundError: \[Errno 2\] No such file or directory: "
                      r"['\"]\/srv\/stockagent-packed\/objects\/inventories\/([0-9a-f]{2})\/"
                      r"([0-9a-f]{64})\.jsonl\.gz['\"]", stderr)
    return bool(match and match.group(2).startswith(match.group(1)))


def dataset_publication_owner_busy(stderr: str, dataset: str) -> bool:
    """Identify only this dataset's canonical lock, never a shared lock."""
    if not isinstance(dataset, str) or re.fullmatch(r'[a-z0-9][a-z0-9-]{0,127}', dataset) is None:
        return False
    if dataset == 'retention-global':
        return False  # Its generated name collides with the shared owner.
    expected = f'PackedPublicationBusy: publication owner busy: publish-{dataset}.lock'
    owner_errors = [line.strip() for line in stderr.splitlines()
                    if line.strip().startswith('PackedPublicationBusy: publication owner busy: ')]
    return owner_errors == [expected]


def private_source_enrollment_sha() -> str:
    values = [stream_repack.PRIVATE_REPACK_POLICY, *stream_repack.PRIVATE_REPACK_EXCLUDED_TOKENS,
              *sorted(stream_repack.PRIVATE_REPACK_DATASETS)]
    return hashlib.sha256('\n'.join(values).encode()).hexdigest()


def testing_disposition():
    """Read the existing exact discard policy; invalid policy excludes nothing."""
    from stockagent.data_sync.testing_disposition import DEFAULT_POLICY, load_policy
    try:
        return load_policy(DEFAULT_POLICY) if DEFAULT_POLICY.exists() else None
    except (OSError, ValueError, TypeError, KeyError, SnapshotError) as error:
        print('testing disposition unavailable: '+type(error).__name__, file=sys.stderr)
        return None


def discovery_is_fresh(value, now: float, *, disposition=None) -> bool:
    if not isinstance(value, dict) or value.get('contract') != DISCOVERY_CONTRACT:
        return False
    next_scan = value.get('next_scan_epoch')
    return (value.get('private_source_enrollment_sha256') == private_source_enrollment_sha()
            and value.get('testing_disposition_identity_sha256') ==
                (disposition.get('identity_sha256') if disposition else None)
            and type(next_scan) in (int, float) and math.isfinite(next_scan) and next_scan > now)


# Large immutable catalogs can legitimately exceed 15 minutes on the HDD.
# The systemd maintenance window remains the outer bound; a shorter child
# timeout caused complete scans to restart without ever reaching deletion.
RETIRE_CHILD_TIMEOUT_SECONDS = 6900
MAX_CONSECUTIVE_DIRECT_RETIREMENTS = 4
# A positive observation only delays the exact pinned item; missing or stale
# hints always return to the original owner and its full locked checks.
PIN_HINT_MAX_AGE_SECONDS = 120
PIN_HINT_RETRY_SECONDS = 300
RETIRE_IMPLEMENTATION_SHA256 = hashlib.sha256(b''.join(path.read_bytes() for path in (
    ROOT/'scripts/retire_stream_repacked_release.py', ROOT/'scripts/retire_superseded_repack_history.py',
    ROOT/'scripts/retire_binance_archive_semantic.py', ROOT/'scripts/retire_latest_fixed_generation.py',
    ROOT/'scripts/promote_latest_fixed_generation.py',
    ROOT/'scripts/deduplicate_latest_fixed_generation.py', ROOT/'stockagent/data_sync/shared_object_dedup.py',
    ROOT/'stockagent/data_sync/semantic_link_retirement.py', ROOT/'stockagent/data_sync/semantic_link_promotion.py',
    ROOT/'stockagent/data_sync/latest_repack_successor.py',
    ROOT/'stockagent/data_sync/repack_retirement.py', ROOT/'stockagent/data_sync/superseded_repack_history.py',
    ROOT/'stockagent/data_sync/private_handoff_retirement.py',
    ROOT/'stockagent/data_sync/private_repack_history.py',
))).hexdigest()


def run_observed(command, *, on_heartbeat, timeout=None, progress_probe=None):
    """Heartbeat a child; optionally bound inactivity rather than total time.

    Only callers with a source-bound progress probe may extend the deadline.
    A heartbeat or a rewritten timestamp alone never proves forward progress.
    """
    started = time.monotonic()
    last_progress = started
    previous = progress_probe() if progress_probe is not None else None
    with subprocess.Popen(command, cwd=ROOT, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                          text=True) as process:
        while True:
            basis = last_progress if progress_probe is not None else started
            remaining = None if timeout is None else timeout - (time.monotonic()-basis)
            if remaining is not None and remaining <= 0:
                process.kill()
                process.communicate()
                raise subprocess.TimeoutExpired(command, timeout)
            try:
                stdout, stderr = process.communicate(timeout=min(30, remaining) if remaining else 30)
                return subprocess.CompletedProcess(command, process.returncode, stdout, stderr)
            except subprocess.TimeoutExpired:
                if progress_probe is not None:
                    current = progress_probe()
                    if current is not None and current != previous:
                        previous, last_progress = current, time.monotonic()
                on_heartbeat()


def discover_fixed_sources(workspace: Path, cold: Path, *, catalog=None) -> list[dict]:
    """Find complete E copies with the same shape as a current D release.

    This is a cheap candidate search, not a content acceptance. The original
    publisher binds the D head and compares all names and SHA while packing.
    FinLab/FinMind/TEJ remain with their concurrent original migration owner.
    """
    catalog = _load_catalog(DEFAULT_CATALOG) if catalog is None else catalog
    if cold == Path('/srv/stockagent-packed'):
        _check_d_primary_mount(cold)
    owners = {str(_source_path(item).absolute()): item for item in catalog}
    root = workspace/'migration'
    intents = sorted(root.glob('*/intent.json'))
    if len(intents) > 4096:
        raise SnapshotError('too many E migration intents to discover safely')
    result = {}
    for path in intents:
        try:
            if path.is_symlink() or (path.parent/'copy-result.json').is_symlink():
                continue
            if path.stat().st_size > 128 * 1024 or (path.parent/'copy-result.json').stat().st_size > 128 * 1024:
                continue
            intent = json.loads(path.read_text())
            copied = json.loads((path.parent/'copy-result.json').read_text())
            if not isinstance(intent, dict) or not isinstance(copied, dict):
                continue
            item = owners.get(intent.get('source'))
            if not item:
                continue
            name = item.get('dataset', '')
            if any(token in name.lower() for token in ('finlab', 'finmind', 'tej')):
                continue
            source = Path(intent['destination'])
            if (copied.get('contract') != 'workspace_exact_relocation_v1'
                    or copied.get('state') != 'exact_destination_readback_verified'
                    or copied.get('excluded_subtrees', []) or copied.get('source_namespace_unchanged') is not True
                    or not source.is_absolute() or not source.is_relative_to(workspace)
                    or source.is_symlink() or source.resolve(strict=True) != source or not source.is_dir()):
                continue
            # Candidate discovery needs only the fixed head and manifest.
            # The actual publisher checks object presence after admission;
            # stat-ing every cold object every 15 minutes creates HDD seeks.
            latest = resolve_latest_packed(cold, name, require_objects=False)
            manifest = latest.manifest
            if (manifest['archive'].get('new_pack_layout') == 'source-dataset-v1'
                    or copied.get('files') != manifest['source']['files']
                    or copied.get('logical_bytes') != manifest['source']['logical_bytes']):
                continue
            stamp = copied.get('observed_at_epoch')
            stamp = float(stamp) if type(stamp) in (int, float) and 0 <= stamp < 2**53 else 0.0
            candidate = {'dataset': name, 'source': str(source),
                         'expected_manifest_sha256': latest.manifest_sha256}
            if name not in result or (stamp, str(source)) > (result[name][0], result[name][1]['source']):
                result[name] = (stamp, candidate)
        except (OSError, ValueError, RuntimeError, KeyError, TypeError):
            continue
    # Stable ordering makes the next candidate reproducible across scans.
    return [result[name][1] for name in sorted(result)]


def source_local_compaction_worthwhile(manifest: dict) -> bool:
    """Spot a large pack pinned by a tiny unchanged tail after an update.

    The arithmetic is only a scheduling heuristic; the original repacker and
    retirement owner still verify their fixed generation and all references.
    Data that compresses below its logical size is intentionally not guessed.
    """
    archive, source = manifest.get('archive', {}), manifest.get('source', {})
    logical, files = source.get('logical_bytes'), source.get('files')
    stored, reused = archive.get('stored_bytes'), archive.get('reused_files')
    if any(type(value) is not int or value < 0 for value in (logical, files, stored, reused)):
        return False
    return (archive.get('new_pack_layout') == 'source-dataset-v1' and files >= 10
            and reused > 0 and reused * 10 <= files and stored > logical * 3 // 2
            and stored - logical >= 64 * 1024**2)


def source_layout_confirmed(cold: Path, dataset: str, manifest: dict, *, limit=32,
                            workspace=Path('/srv/stockagent-workspace')) -> bool:
    """Prove reused objects originate in source-layout generations, metadata only.

    A current manifest's write policy describes newly created objects, not the
    old packs it still references. Missing history is unknown and remains a
    repack candidate; the fixed-source owner still checks every member.
    """
    active, seen = None, set()
    for _ in range(limit):
        try:
            archive = manifest['archive']
            objects = archive.get('objects')
            if active is not None:
                if not isinstance(objects, list):
                    return False
                references = {item['sha256'] for item in objects if isinstance(item, dict) and 'sha256' in item}
                if len(references) != len(objects):
                    return False
                active &= references
                if not active:
                    return True
            if archive.get('new_pack_layout') != 'source-dataset-v1':
                return False
            if archive.get('external_reused_files', 0):
                # The ordinary zero-reuse shortcut cannot establish the
                # physical origin of another dataset's objects. Follow the
                # durable, fixed, same-private-authority comparison instead.
                from stockagent.data_sync import private_subset_reuse as subset
                old = manifest.get('metadata', {}).get('physical_repack_original_manifest_sha256')
                if (cold != stream_repack.PRIVATE_REPACK_ROOT or dataset not in subset.REVIEWED_PAIRS
                        or not isinstance(old, str) or re.fullmatch(r'[a-f0-9]{64}', old) is None):
                    return False
                stage = Path(workspace)/'migration/stream-source-repack-v1'/dataset/old
                prepared = stream_repack._prepared_metadata(stage, dataset, old)
                return (prepared.get('packed_format') == subset.FORMAT
                        and prepared.get('donor_source_layout_confirmed') is True
                        and prepared.get('archive') == archive)
            reused = archive.get('reused_files')
            if type(reused) is not int or reused < 0:
                return False
            selection = manifest.get('source', {}).get('selection') or {}
            if not isinstance(selection, dict):
                return False
            has_virtual_sources = bool(selection.get('retained_source_contract') or selection.get('incremental_base'))
            if reused == 0 and not has_virtual_sources:
                return True
            if reused == 0 and has_virtual_sources:
                # Exact repacks keep the original source-selection provenance,
                # even though all physical objects were rebuilt. The sealed
                # original-owner transaction proves their layout independently
                # of an older manifest that retention may already have removed.
                from stockagent.data_sync.dataset_migration import _exact_repack_refresh_successor
                original = manifest.get('metadata', {}).get('physical_repack_original_manifest_sha256')
                if isinstance(original, str) and re.fullmatch('[a-f0-9]{64}', original):
                    stage = Path(workspace)/'migration/stream-source-repack-v1'/dataset/original
                    prepared = stream_repack._prepared_metadata(stage, dataset, original)
                    private = cold == stream_repack.PRIVATE_REPACK_ROOT
                    proof = _exact_repack_refresh_successor(cold, Path(workspace),
                        {'dataset': dataset, 'publish': not private},
                        {'domain': 'host_private' if private else 'shared',
                         'snapshot_id': prepared['old_snapshot_id'], 'manifest_sha256': original})
                    if proof is not None:
                        from stockagent.data_sync import packed_snapshots as packed
                        if proof['manifest_sha256'] == hashlib.sha256(packed._canonical_json_bytes(manifest)).hexdigest():
                            return True
            if not isinstance(objects, list):
                return False
            references = {item['sha256'] for item in objects if isinstance(item, dict) and 'sha256' in item}
            if len(references) != len(objects):
                return False
            active = references if active is None else active & references
            if not active:
                return True
            base = archive.get('base_snapshot_id')
            if not isinstance(base, str) or base in seen:
                return False
            seen.add(base)
            manifest = resolve_packed_snapshot_id(cold, dataset, base, require_objects=False).manifest
        except (OSError, ValueError, RuntimeError, KeyError, TypeError):
            return False
    return False


def discover_cold_sources(workspace: Path, cold: Path, *, catalog=None) -> list[dict]:
    """Find catalogued D heads without scanning their potentially huge object sets.

    Admission and content checks belong to the streaming publisher.  A broken
    or temporarily absent head must not prevent independent datasets from
    being scheduled. Other active provider migration owners remain excluded.
    """
    catalog = _load_catalog(DEFAULT_CATALOG) if catalog is None else catalog
    if cold == Path('/srv/stockagent-packed'):
        _check_d_primary_mount(cold)
    result = []
    for item in catalog:
        name = item.get('dataset')
        if (item.get('publish') is False or not isinstance(name, str)
                or any(token in name.lower() for token in ('finlab', 'finmind', 'tej'))):
            continue
        try:
            latest = resolve_latest_packed(cold, name, require_objects=False)
            manifest = latest.manifest
            if (not source_local_compaction_worthwhile(manifest)
                    and source_layout_confirmed(cold, name, manifest)):
                continue
            logical = manifest['source']['logical_bytes']
            files = manifest['source']['files']
            if type(logical) is not int or logical < 0 or type(files) is not int or files < 0:
                raise SnapshotError('invalid source counts')
            result.append({'dataset': name,
                'source': str(workspace/'migration/stream-source-repack-v1'/name/latest.manifest_sha256),
                'expected_manifest_sha256': latest.manifest_sha256, 'origin': 'cold',
                'logical_bytes': logical, 'files': files})
        except (OSError, ValueError, RuntimeError, KeyError, TypeError):
            continue
    return sorted(result, key=lambda row: (row['logical_bytes'], row['files'], row['dataset']))


def private_state_pin_hint(private: Path, name: str, latest, cache: dict) -> bool:
    """Delay a state snapshot already fixed by a current parent, never authorize deletion."""
    match = re.fullmatch(r'(.+)-state-[a-f0-9]{12}', name)
    if match is None:
        return False
    try:
        from stockagent.data_sync.private_handoff_retirement import _pending_preparation_protections
        if _pending_preparation_protections(private, name).get(latest.manifest.get('snapshot_id')) == latest.manifest_sha256:
            return True
    except (OSError, ValueError, RuntimeError, KeyError, TypeError):
        pass  # This is only a hint; the retirement owner's final check is mandatory.
    parent = match.group(1)
    owners = [parent]
    from stockagent.data_sync import private_source_links as links
    if parent == links.TARGET_DATASET:
        owners.append('local-tw-futures')
    for owner in owners:
        if owner not in cache:
            try:
                cache[owner] = resolve_latest_packed(private, owner, require_objects=False)
            except (OSError, ValueError, RuntimeError, KeyError, TypeError):
                cache[owner] = None
        selected = cache[owner]
        if selected is None:
            continue
        try:
            raw = selected.manifest.get('metadata', {}).get('consistent_sqlite_bindings', '[]')
            bindings = json.loads(raw)
            if not isinstance(bindings, list) or len(bindings) > 100000:
                continue
            bindings.extend(links.source_link_dependencies(links.manifest_source_links(selected)))
            if any(isinstance(row, dict) and row.get('dataset') == name
                   and row.get('snapshot_id') == latest.manifest.get('snapshot_id')
                   and row.get('manifest_sha256') == latest.manifest_sha256 for row in bindings):
                return True
        except (OSError, ValueError, RuntimeError, KeyError, TypeError):
            continue
    return False


def discover_private_sources(workspace: Path, cold: Path, *, private=None, cursor='', limit=128,
                             with_cursor=False, on_error=lambda _error: None):
    """Rotate bounded private heads under their original local-only owner."""
    if not isinstance(cursor, str) or type(limit) is not int or not 1 <= limit <= 512:
        raise SnapshotError('invalid private discovery cursor or batch limit')
    if private is None:
        try:
            private = private_history_root(cold, version_policy(ROOT), initialize=False)
        except (OSError, ValueError, RuntimeError) as error:
            # An unavailable private authority must not block public sources.
            print('private source discovery deferred: '+str(error), file=sys.stderr)
            on_error({'dataset': None, 'reason': 'private_authority_unavailable'})
            return ([], cursor, False) if with_cursor else []
    if isinstance(stream_repack.PRIVATE_REPACK_DATASETS, stream_repack._PrivateRepackDatasets):
        heads = Path(private)/'heads'
        try:
            if (Path(private).absolute() != stream_repack.PRIVATE_REPACK_ROOT
                    or heads.is_symlink() or not heads.is_dir() or heads.resolve(strict=True) != heads):
                raise SnapshotError('private head authority is unavailable or redirected')
        except (OSError, SnapshotError):
            on_error({'dataset': None, 'reason': 'private_authority_unavailable'})
            return ([], cursor, False) if with_cursor else []
    from stockagent.data_sync.private_handoff_retirement import check_dependency_metadata
    names = sorted(stream_repack.PRIVATE_REPACK_DATASETS)
    start = next((index for index, name in enumerate(names) if name > cursor), 0)
    ordered = names[start:]+names[:start]
    result, parent_cache = [], {}
    last, examined = cursor, 0
    # Bad or already-converted heads must not turn one refresh into an
    # unbounded HDD census; the persisted cursor visits the remainder.
    for name in ordered[:max(128, limit*4)]:
        last, examined = name, examined+1
        try:
            latest = resolve_latest_packed(private, name, require_objects=False)
            manifest = latest.manifest
            if not stream_repack.private_repack_source_allowed(name, manifest):
                continue
            check_dependency_metadata(manifest)
            if source_layout_confirmed(private, name, manifest):
                continue
            logical, files = manifest['source']['logical_bytes'], manifest['source']['files']
            if type(logical) is not int or logical < 0 or type(files) is not int or files < 0:
                raise SnapshotError('invalid private source counts')
            row = {'dataset': name,
                'source': str(workspace/'migration/stream-source-repack-v1'/name/latest.manifest_sha256),
                'expected_manifest_sha256': latest.manifest_sha256, 'origin': 'private_cold',
                'logical_bytes': logical, 'files': files}
            if private_state_pin_hint(private, name, latest, parent_cache):
                row['dependency_pin_hint'] = True
            result.append(row)
            if len(result) >= limit:
                break
        except (OSError, ValueError, RuntimeError, KeyError, TypeError) as error:
            on_error({'dataset': name, 'reason': ('private_dependency_contract_unknown'
                if 'unsupported private dependency metadata' in str(error) else 'private_head_unavailable')})
            continue
    return (result, last, examined < len(ordered)) if with_cursor else result


def discover_other_cold_sources(workspace: Path, cold: Path, *, catalog, cursor='', limit=16,
                                disposition=None):
    """Rotate a bounded batch through other immutable D heads.

    This includes older per-folder and study releases without repeatedly
    reading every large manifest. Pins and live consumers are checked by the
    separate retirement owner before any old name is removed.
    """
    from stockagent.data_sync.testing_disposition import manifest_discarded
    registered = {item.get('dataset') for item in catalog}
    root = cold/'heads'
    if root.is_symlink() or not root.is_dir() or root.resolve(strict=True) != root:
        raise SnapshotError('cold head namespace is missing or redirected')
    names = sorted(item.name for item in root.iterdir() if not item.is_symlink() and item.is_dir())
    if len(names) > 100000:
        raise SnapshotError('cold head namespace exceeds the bounded sweep')
    start = next((index for index, name in enumerate(names) if name > cursor), 0)
    ordered = names[start:] + names[:start]
    result, last = [], cursor
    for name in ordered:
        last = name
        if (name in registered or not re.fullmatch(r'[a-z0-9][a-z0-9-]{0,127}', name)
                or any(token in name.lower() for token in ('finlab', 'finmind', 'tej', 'factorized'))):
            continue
        try:
            latest = resolve_latest_packed(cold, name, require_objects=False)
            manifest = latest.manifest
            if disposition and manifest_discarded(manifest, disposition):
                continue
            logical, files = manifest['source']['logical_bytes'], manifest['source']['files']
            origin = ' '.join(str(value).lower() for value in (
                manifest.get('source', {}).get('snapshot_root_name', ''),
                manifest.get('metadata', {}).get('source_relative_root', ''),
                manifest.get('metadata', {}).get('artifact_relative_root', '')))
            if (source_layout_confirmed(cold, name, manifest)
                    or any(token in origin for token in ('finlab', 'finmind', 'tej', 'factorized'))
                    or type(logical) is not int or logical < 0 or type(files) is not int or files < 0):
                continue
            result.append({'dataset': name,
                'source': str(workspace/'migration/stream-source-repack-v1'/name/latest.manifest_sha256),
                'expected_manifest_sha256': latest.manifest_sha256, 'origin': 'cold',
                'logical_bytes': logical, 'files': files})
            if len(result) >= limit:
                break
        except (OSError, ValueError, RuntimeError, KeyError, TypeError):
            continue
    return sorted(result, key=lambda row: (row['logical_bytes'], row['files'], row['dataset'])), last


def validate_entries(rows, workspace: Path, *, allow_generations=False) -> list[dict]:
    if not isinstance(rows, list) or len(rows) > 1024:
        raise SnapshotError('invalid exact-repack queue')
    seen, names = set(), set()
    for row in rows:
        if not isinstance(row, dict):
            raise SnapshotError('invalid exact-repack queue member')
        name, sha, source = (row.get(k) for k in ('dataset', 'expected_manifest_sha256', 'source'))
        if (not isinstance(name, str) or not re.fullmatch(r'[a-z0-9][a-z0-9-]{0,127}', name)
                or not isinstance(sha, str) or not re.fullmatch(r'[a-f0-9]{64}', sha)
                or not isinstance(source, str) or not Path(source).is_absolute()
                or not Path(source).is_relative_to(workspace) or '..' in Path(source).parts):
            raise SnapshotError('invalid exact-repack queue member')
        origin = row.get('origin', 'workspace')
        if origin not in ('workspace', 'cold', 'private_cold'):
            raise SnapshotError('invalid exact-repack queue origin')
        if (name, sha) in seen or name in names and not allow_generations:
            raise SnapshotError('invalid exact-repack queue member')
        if row.get('origin') in ('cold', 'private_cold'):
            expected_path = workspace/'migration/stream-source-repack-v1'/name/sha
            if (Path(source) != expected_path
                    or row.get('origin') == 'private_cold' and name not in stream_repack.PRIVATE_REPACK_DATASETS
                    or any(type(row.get(key)) is not int or row[key] < 0
                           for key in ('logical_bytes', 'files'))):
                raise SnapshotError('invalid streaming-repack queue member')
        if ('dependency_pin_hint' in row and
                (row['dependency_pin_hint'] is not True or origin != 'private_cold')):
            raise SnapshotError('invalid private dependency hint')
        superseded = row.get('superseded_by_manifest_sha256')
        if superseded is not None and (not isinstance(superseded, str)
                or re.fullmatch(r'[a-f0-9]{64}', superseded) is None or superseded == sha):
            raise SnapshotError('invalid exact-repack superseding generation')
        seen.add((name, sha))
        names.add(name)
    return rows


def load_queue(path: Path, workspace: Path) -> list[dict]:
    value = json.loads(path.read_text())
    if not isinstance(value, dict) or value.get('contract') != CONTRACT:
        raise SnapshotError('invalid exact-repack queue')
    return validate_entries(value.get('entries'), workspace)


def prior_published(root: Path, row: dict) -> bool:
    directory = root/row['dataset']
    if not directory.is_dir() or directory.is_symlink():
        return False
    receipts = list(directory.glob('*.json'))
    if len(receipts) > 256:
        raise SnapshotError('too many repack receipts for one dataset')
    for path in receipts:
        if path.is_symlink() or not re.fullmatch(r'[a-f0-9]{64}\.json', path.name):
            continue
        try:
            receipt = json.loads(path.read_text())
        except (OSError, ValueError):
            continue
        if not isinstance(receipt, dict):
            continue
        if (receipt.get('contract') == 'stockagent.source_local_exact_repack.v1'
                and receipt.get('state') == 'new_D_head_committed_from_exact_E_source'
                and receipt.get('dataset') == row['dataset']
                and (receipt.get('private') is True) == (row.get('origin') == 'private_cold')
                and receipt.get('old_manifest_sha256') == row['expected_manifest_sha256']
                and receipt.get('new_manifest_sha256') == path.stem):
            return True
    return False


def discover_pending_retirements(root: Path) -> list[dict]:
    """Only receipts produced by the streamed repacker can authorize this path."""
    result = []
    for directory in sorted(root.iterdir()):
        if directory.is_symlink() or not directory.is_dir():
            continue
        receipts = list(directory.glob('*.json'))
        if len(receipts) > 256:
            continue
        for path in receipts:
            if path.is_symlink() or not re.fullmatch(r'[a-f0-9]{64}\.json', path.name):
                continue
            try:
                value = json.loads(path.read_text())
                if not isinstance(value, dict):
                    continue
                old = value.get('old_manifest_sha256')
                size = value.get('logical_bytes')
                from stockagent.data_sync import private_subset_reuse as subset
                if (value.get('contract') != 'stockagent.source_local_exact_repack.v1'
                        or value.get('state') != 'new_D_head_committed_from_exact_E_source'
                        or value.get('source_mode') not in {'D_streamed_to_E_dataset_pack', subset.SOURCE_MODE}
                        or (value.get('source_mode') == subset.SOURCE_MODE
                            and (value.get('private') is not True or directory.name not in subset.REVIEWED_PAIRS))
                        or (value.get('private') is True and directory.name not in stream_repack.PRIVATE_REPACK_DATASETS)
                        or value.get('dataset') != directory.name or value.get('new_manifest_sha256') != path.stem
                        or not isinstance(old, str) or not re.fullmatch(r'[a-f0-9]{64}', old)
                        or type(size) is not int or size < 0):
                    continue
                if value.get('old_release_removed') is False:
                    mode = 'direct'
                elif value.get('old_release_removed') is True:
                    next_epoch = value.get('history_sweep_next_epoch', 0)
                    if type(next_epoch) not in (int, float) or next_epoch > time.time():
                        continue
                    mode = 'history'
                else:
                    continue
                row = {'dataset': directory.name, 'old_manifest_sha256': old,
                       'logical_bytes': size, 'mode': mode}
                if value.get('private') is True:
                    row['private'] = True
                result.append(row)
            except (OSError, ValueError, TypeError):
                continue
    # Give the largest newly superseded source a turn first: its source bytes
    # are an upper bound, not a promise of reclaimable encoded bytes. The
    # individual owner still checks actual shared objects, heads and pins.
    # Older histories keep ascending order for their round-robin cursor.
    return sorted(result, key=lambda row: (row['mode'] != 'direct',
        -row['logical_bytes'] if row['mode'] == 'direct' else row['logical_bytes'],
        row['dataset'], row['old_manifest_sha256']))


def discover_prepared_history_retirements(root: Path) -> list[dict]:
    """Find sealed, capacity-deferred builds without occupying the build owner.

    The ordinary retirement worker owns these sweeps as well.  A running
    build is deliberately excluded: an older already-running queue may still
    do its own post-build sweep before publishing its final status.
    """
    source = root/'queue-status.json'
    if source.is_symlink():
        raise SnapshotError('prepared history source queue redirected')
    try:
        status = json.loads(source.read_text())
    except FileNotFoundError:
        return []
    if not isinstance(status, dict) or status.get('contract') != CONTRACT+'.status':
        return []
    entries = status.get('entries')
    if not isinstance(entries, list) or len(entries) > 1024:
        raise SnapshotError('prepared history source queue invalid')
    stage_root = root.parent/'stream-source-repack-v1'
    result = []
    for item in entries:
        if (not isinstance(item, dict) or item.get('origin') not in ('cold', 'private_cold')
                or item.get('state') not in ('waiting', 'retrying')
                or item.get('E_prepared') is not True):
            continue
        try:
            name, digest = item.get('dataset'), item.get('expected_manifest_sha256')
            if (not isinstance(name, str) or not re.fullmatch(r'[a-z0-9][a-z0-9-]{0,127}', name)
                    or not isinstance(digest, str) or not re.fullmatch(r'[a-f0-9]{64}', digest)
                    or item['origin'] == 'private_cold' and name not in stream_repack.PRIVATE_REPACK_DATASETS):
                raise SnapshotError('invalid prepared history identity')
            # A stale upstream row may outlive the successful D receipt and
            # E-stage cleanup. Do not treat the deliberately removed stage
            # as an error or rediscover it as pending work.
            if prior_published(root, item):
                continue
            stage = stage_root/name/digest
            if stage.is_symlink() or stage.resolve(strict=True) != stage:
                raise SnapshotError('prepared history stage redirected')
            prepared = stream_repack._read_prepared(stage, name, digest)
            sweep = stage/'prepared-history-sweep.json'
            if sweep.is_symlink():
                raise SnapshotError('prepared history status redirected')
            if sweep.is_file():
                previous = json.loads(sweep.read_text())
                if (not isinstance(previous, dict)
                        or previous.get('contract') != 'stockagent.source_local_prepared_history.v1'
                        or previous.get('dataset') != name or previous.get('old_manifest_sha256') != digest):
                    raise SnapshotError('prepared history status identity differs')
                next_time = previous.get('history_sweep_next_epoch', 0)
                if type(next_time) not in (int, float):
                    raise SnapshotError('prepared history retry clock invalid')
                if next_time > time.time():
                    continue
            row = {'dataset': name, 'old_manifest_sha256': digest,
                   'logical_bytes': prepared['logical_bytes'], 'mode': 'prepared_history'}
            if item['origin'] == 'private_cold':
                row['private'] = True
            result.append(row)
        except (OSError, ValueError, TypeError, KeyError, SnapshotError) as error:
            # One broken stage cannot block unrelated completed generations.
            print('prepared history discovery deferred: '+type(error).__name__, file=sys.stderr)
    return sorted(result, key=lambda row: (row['logical_bytes'], row['dataset'], row['old_manifest_sha256']))


def discover_semantic_retirements(root: Path) -> list[dict]:
    """Schedule the enrolled fixed superset through the same retirement owner."""
    directory = root.parent/'handoff-index-archive-v1/binance-public-archive'
    if directory.is_symlink() or not directory.is_dir():
        return []
    paths = list(directory.glob('semantic-promotion-*.json'))
    if len(paths) > 64:
        raise SnapshotError('semantic promotion receipt set exceeds its bound')
    result = []
    for path in paths:
        try:
            if path.is_symlink() or path.stat().st_size > 1024**2:
                continue
            value = json.loads(path.read_bytes())
            old = value.get('old_manifest_sha256')
            comparison = value.get('comparison_identity_sha256')
            size = value.get('manifest', {}).get('source', {}).get('logical_bytes')
            if (value.get('contract') != 'stockagent.semantic_link_promotion.v1'
                    or value.get('state') != 'committed' or value.get('dataset') != 'binance-public-archive'
                    or value.get('old_sources_deleted') is not False
                    or not isinstance(old, str) or re.fullmatch('[a-f0-9]{64}', old) is None
                    or not isinstance(comparison, str) or path.name != 'semantic-promotion-'+comparison+'.json'
                    or re.fullmatch('[a-f0-9]{64}', comparison) is None or type(size) is not int or size < 0):
                continue
            result.append({'dataset': 'binance-public-archive', 'old_manifest_sha256': old,
                           'logical_bytes': size, 'mode': 'semantic'})
        except (OSError, ValueError, TypeError):
            continue
    return result


def discover_latest_retirements(root: Path) -> list[dict]:
    """Retire only an explicitly selected and committed newer source head."""
    result = []
    for dataset in sorted(semantic_link_promotion.FAST_LATEST_DATASETS):
        directory = root.parent/'handoff-index-archive-v1'/dataset
        if directory.is_symlink() or not directory.is_dir():
            continue
        paths = list(directory.glob('latest-promotion-*.json'))
        if len(paths) > 64:
            raise SnapshotError('latest fixed promotion receipt set exceeds its bound')
        for path in paths:
            try:
                if path.is_symlink() or path.stat().st_size > 1024**2:
                    continue
                value = json.loads(path.read_bytes())
                old, comparison = value.get('old_manifest_sha256'), value.get('comparison_identity_sha256')
                size = value.get('manifest', {}).get('source', {}).get('logical_bytes')
                conflicts = value.get('uncompared_changed_members')
                if (value.get('contract') != 'stockagent.latest_fixed_link_promotion.v1'
                        or value.get('state') != 'committed' or value.get('dataset') != dataset
                        or value.get('old_sources_deleted') is not False
                        or value.get('changed_member_values_compared') is not False
                        or value.get('old_historical_value_coverage') != 'not_compared'
                        or not isinstance(old, str) or re.fullmatch('[a-f0-9]{64}', old) is None
                        or not isinstance(comparison, str) or path.name != 'latest-promotion-'+comparison+'.json'
                        or re.fullmatch('[a-f0-9]{64}', comparison) is None
                        or type(size) is not int or size < 0 or type(conflicts) is not int or conflicts < 0):
                    continue
                result.append({'dataset': dataset, 'old_manifest_sha256': old,
                               'logical_bytes': size, 'mode': 'latest', 'uncompared_changed_members': conflicts})
            except (OSError, ValueError, TypeError):
                continue
    return result


def discover_latest_shared_objects(root: Path) -> list[dict]:
    """Resume the fixed duplicate transaction under the existing queue owner."""
    result = []
    for dataset in sorted(semantic_link_promotion.FAST_LATEST_DATASETS):
        directory = root.parent/'handoff-index-archive-v1'/dataset
        if directory.is_symlink() or not directory.is_dir():
            continue
        paths = list(directory.glob('latest-promotion-*.json'))
        if len(paths) > 64:
            raise SnapshotError('latest fixed promotion receipt set exceeds its bound')
        for path in paths:
            try:
                if path.is_symlink() or path.stat().st_size > 1024**2:
                    continue
                value = json.loads(path.read_bytes())
                old, comparison = value.get('old_manifest_sha256'), value.get('comparison_identity_sha256')
                pending = value.get('duplicate_encoded_bytes_pending')
                if (value.get('contract') != 'stockagent.latest_fixed_link_promotion.v1'
                        or value.get('state') != 'committed' or value.get('dataset') != dataset
                        or value.get('old_sources_deleted') is not True
                        or not isinstance(old, str) or re.fullmatch('[a-f0-9]{64}', old) is None
                        or not isinstance(comparison, str) or re.fullmatch('[a-f0-9]{64}', comparison) is None
                        or path.name != 'latest-promotion-'+comparison+'.json'
                        or type(pending) is not int or pending < 0):
                    continue
                journal = directory/('shared-objects-'+comparison+'.json')
                unfinished = False
                if journal.exists() or journal.is_symlink():
                    if journal.is_symlink() or journal.stat().st_size > 4*1024**2:
                        raise SnapshotError('shared-object journal is redirected or oversized')
                    record = json.loads(journal.read_bytes())
                    unfinished = record.get('state') != 'completed'
                if pending or unfinished:
                    result.append({'dataset': dataset, 'old_manifest_sha256': old,
                                   'logical_bytes': pending, 'mode': 'dedup'})
            except (OSError, ValueError, TypeError):
                continue
    return result


def run_one_retirement(root: Path, *, on_selected=None, positive_pin_hints=None,
                       busy_publication_scopes=None) -> bool:
    """Let either scheduler attempt one retirement under the same local owner."""
    root = Path(root).absolute()
    if root.is_symlink() or not root.is_dir() or root.resolve(strict=True) != root:
        raise SnapshotError('retirement queue root is missing or redirected')
    descriptor = os.open(root/'retirement-queue-owner.lock',
                         os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW | os.O_CLOEXEC, 0o600)
    try:
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return False
        return _run_one_retirement_owned(root, on_selected=on_selected,
                                         positive_pin_hints=positive_pin_hints,
                                         busy_publication_scopes=busy_publication_scopes)
    finally:
        os.close(descriptor)


def _selected_prefetch_with_heartbeat(callback, selected, save, *, interval=20.0):
    """Keep queue liveness distinct from byte progress while preparing a hint."""
    stop = threading.Event()
    failures = []
    def pulse():
        while not stop.wait(interval):
            try:
                save()
            except (OSError, ValueError) as error:
                failures.append(error)
                break
    worker = threading.Thread(target=pulse, name='retirement-prefetch-heartbeat', daemon=True)
    worker.start()
    try:
        callback(dict(selected))
    finally:
        stop.set()
        worker.join()
    if failures:
        raise failures[0]


def retirement_cooldown_active(row: dict, now: float) -> bool:
    """Use only a finite retry from this exact retirement implementation."""
    retry = row.get('retry_after_epoch') if isinstance(row, dict) else None
    if type(retry) not in (int, float) or row.get('execution_sha256') != RETIRE_IMPLEMENTATION_SHA256:
        return False
    try:
        return math.isfinite(retry) and retry > now
    except OverflowError:
        return False  # Malformed metadata must not hide a ready item.


def positive_pin_hint_applies(hints, row: dict, now: float) -> bool:
    """Use a fresh exact positive only for scheduling, never as deletion proof."""
    if not isinstance(hints, dict):
        return False
    stamp, keys = hints.get('observed_at_epoch'), hints.get('keys')
    if (type(stamp) not in (int, float) or not isinstance(keys, frozenset) or len(keys) > 4096
            or row.get('mode') != 'direct' or row.get('private') is True):
        return False
    try:
        if not math.isfinite(stamp) or not 0 <= now-stamp <= PIN_HINT_MAX_AGE_SECONDS:
            return False
    except OverflowError:
        return False
    return (row.get('dataset'), row.get('old_manifest_sha256')) in keys


_RETIREMENT_HEAD_BLOCKED_HINTS = set()


def handoff_completed_identity(workspace: Path, dataset: str, old_sha: str) -> str | None:
    """A completed exact peer handoff wakes one pin retry, never authorizes it."""
    try:
        from stockagent.data_sync import authoritative_head_handoff as handoff, packed_snapshots
        name = packed_snapshots.validate_slug(dataset, 'dataset')
        digest = packed_snapshots._validate_hash(old_sha, 'old manifest SHA')
        stage = Path(workspace)/'migration/authoritative-head-handoffs'/name/digest
        path = stage/'journal.json'
        if path.is_symlink() or not path.is_file() or path.stat().st_size > 65536:
            return None
        value = json.loads(path.read_text())
        request = value.get('request') if isinstance(value, dict) else None
        if (not isinstance(request, dict) or request.get('dataset') != name
                or request.get('old_manifest_sha256') != digest):
            return None
        checked = handoff._record(stage, request)
        return checked['identity_sha256'] if checked['state'] == 'completed' else None
    except (OSError, ValueError, TypeError, KeyError, SnapshotError):
        return None


def retirement_head_released_hint(workspace: str, cold: str, dataset: str, old_sha: str) -> bool:
    """Wake an old-head cooldown on a new head; the child still owns retirement.

    This is a per-process scheduling hint, not an authorization. A new timer
    invocation observes the heads again. Any unavailable or malformed metadata
    keeps the ordinary cooldown and the original owner still rechecks pins,
    references, readers and locks before touching data.
    """
    key = (workspace, cold, dataset, old_sha)
    if key in _RETIREMENT_HEAD_BLOCKED_HINTS:
        return False
    try:
        from stockagent.data_sync import packed_snapshots, repack_retirement
        name = packed_snapshots.validate_slug(dataset, 'dataset')
        digest = packed_snapshots._validate_hash(old_sha, 'old manifest SHA')
        stage = Path(workspace)/'migration/stream-source-repack-v1'/name/digest
        if stage.is_symlink() or stage.resolve(strict=True) != stage:
            return False
        prepared = stream_repack._prepared_metadata(stage, name, digest)
        old_id = packed_snapshots.validate_slug(prepared['old_snapshot_id'], 'old snapshot')
        repack_retirement._require_not_current_head(Path(cold), name, digest, old_id)
        return True  # Do not cache a release: a head may reappear before the child runs.
    except (OSError, ValueError, TypeError, KeyError, SnapshotError):
        if len(_RETIREMENT_HEAD_BLOCKED_HINTS) >= 2048:
            _RETIREMENT_HEAD_BLOCKED_HINTS.clear()
        _RETIREMENT_HEAD_BLOCKED_HINTS.add(key)
        return False


def _retirement_publication_scopes(row: dict) -> frozenset[str]:
    """Identify owner domains for a scheduling hint, not an unlink authority."""
    if row.get('mode') in {'latest', 'dedup', 'semantic'}:
        return frozenset({'public', 'private'})
    return frozenset({'private' if row.get('private') is True else 'public'})


def _run_one_retirement_owned(root: Path, *, on_selected=None, positive_pin_hints=None,
                              busy_publication_scopes=None) -> bool:
    """Perform one exact retirement using the canonical storage owners."""
    if busy_publication_scopes is not None and (not isinstance(busy_publication_scopes, (set, frozenset))
            or not busy_publication_scopes.issubset({'public', 'private'})):
        raise SnapshotError('invalid retirement publication scheduling hint')
    pending = (discover_latest_retirements(root) + discover_latest_shared_objects(root)
               + discover_semantic_retirements(root) + discover_pending_retirements(root)
               + discover_prepared_history_retirements(root))
    path = root/'retirement-queue-status.json'
    try:
        previous = json.loads(path.read_text()) if not path.is_symlink() else {}
    except (OSError, ValueError):
        previous = {}
    if not isinstance(previous, dict) or previous.get('contract') != CONTRACT+'.retirement-status':
        previous = {}
    previous_entries = previous.get('entries', [])
    if not isinstance(previous_entries, list) or len(previous_entries) > 4096:
        previous_entries = []
    prior = {(row.get('dataset'), row.get('old_manifest_sha256'), row.get('mode', 'direct')): row
             for row in previous_entries if isinstance(row, dict) and isinstance(row.get('dataset'), str)
             and isinstance(row.get('old_manifest_sha256'), str) and isinstance(row.get('mode', 'direct'), str)}
    now = time.time()
    states = []
    last_history = previous.get('last_history_sort_key')
    if (not isinstance(last_history, list) or len(last_history) != 3
            or type(last_history[0]) is not int or last_history[0] < 0
            or not all(isinstance(item, str) for item in last_history[1:])):
        last_history = None
    direct_streak = previous.get('consecutive_direct_attempts', 0)
    if type(direct_streak) is not int or not 0 <= direct_streak <= MAX_CONSECUTIVE_DIRECT_RETIREMENTS:
        direct_streak = 0
    for row in pending:
        old = prior.get((row['dataset'], row['old_manifest_sha256'], row['mode']), {})
        retry = old.get('retry_after_epoch')
        cooling = retirement_cooldown_active(old, now)
        hinted_scopes = _retirement_publication_scopes(row)
        if (cooling and old.get('reason') == 'd_e_owner_busy'
                and old.get('owner_lock') == 'publication_scope_hint'
                and busy_publication_scopes is not None and not (hinted_scopes & busy_publication_scopes)):
            # A fresh observation may wake this item immediately. It cannot
            # authorize retirement; the child still takes every original lock.
            cooling = False
        if (cooling and row['mode'] == 'direct' and row.get('private') is not True
                and old.get('reason') == 'retirement_current_head'
                and retirement_head_released_hint(str(root.parent.parent), '/srv/stockagent-packed',
                                                  row['dataset'], row['old_manifest_sha256'])):
            cooling = False
        if (cooling and row['mode'] == 'direct' and row.get('private') is not True
                and old.get('reason') == 'retirement_pinned'):
            identity = handoff_completed_identity(root.parent.parent, row['dataset'], row['old_manifest_sha256'])
            if identity and old.get('handoff_recheck_identity') != identity:
                cooling = False
        if (not cooling and old.get('reason') == 'retirement_pinned'
                and positive_pin_hint_applies(positive_pin_hints, row, now)):
            # The exact original release was observed pinned in this batch.
            # Recheck soon; no worker, lock, pin, or payload is changed here.
            cooling, retry = True, now + PIN_HINT_RETRY_SECONDS
        if cooling:
            item = {**row, 'state': 'waiting_retry', 'reason': old.get('reason', 'repack_temporary_failure'),
                    'retry_after_epoch': retry, 'execution_sha256': RETIRE_IMPLEMENTATION_SHA256}
            if isinstance(old.get('handoff_recheck_identity'), str):
                item['handoff_recheck_identity'] = old['handoff_recheck_identity']
            if old.get('owner_lock') in {'publish_retention_global', 'publication_scope_hint'}:
                item['owner_lock'] = old['owner_lock']
        elif busy_publication_scopes is not None and hinted_scopes & busy_publication_scopes:
            # Other authority domains remain runnable; never repeatedly spawn
            # children that all need the same currently occupied owner.
            item = {**row, 'state': 'waiting_retry', 'reason': 'd_e_owner_busy',
                    'owner_lock': 'publication_scope_hint', 'retry_after_epoch': now+30,
                    'execution_sha256': RETIRE_IMPLEMENTATION_SHA256}
        else:
            item = {**row, 'state': 'waiting'}
        states.append(item)
    ready = [item for item in states if item['state'] == 'waiting']
    chosen = next((item for item in ready if item['mode'] == 'latest'), None)
    if chosen is None:
        chosen = next((item for item in ready if item['mode'] == 'dedup'), None)
    if chosen is None:
        chosen = next((item for item in ready if item['mode'] == 'semantic'), None)
    history = [item for item in ready if item['mode'] == 'history']
    # Direct retirements normally release newly duplicated bytes first. Do
    # not starve existing histories when repacks keep adding direct items:
    # one ready history gets a turn after four direct attempts. The child
    # still performs all of its own current-head, pin and reference checks.
    history_due = bool(history and direct_streak >= MAX_CONSECUTIVE_DIRECT_RETIREMENTS)
    if chosen is None and not history_due:
        chosen = next((item for item in ready if item['mode'] == 'direct'), None)
    if chosen is None and not history_due:
        chosen = next((item for item in ready if item['mode'] == 'prepared_history'), None)
    if chosen is None:
        if history:
            # A just-retired head may have freed little because older
            # revisions still share its objects. Give its history one pass
            # immediately, then return to the round-robin cursor. The exact
            # reference, research-pin and reader checks are unchanged.
            just_retired = {(item.get('dataset'), item.get('old_manifest_sha256'))
                            for item in previous_entries if isinstance(item, dict)
                            and isinstance(item.get('dataset'), str)
                            and isinstance(item.get('old_manifest_sha256'), str)
                            and item.get('mode') == 'direct' and item.get('state') == 'completed'}
            chosen = next((item for item in history
                           if (item['dataset'], item['old_manifest_sha256']) in just_retired), None)
            if chosen is None:
                index = next((i for i, item in enumerate(history) if last_history is not None
                              and (item['logical_bytes'], item['dataset'], item['old_manifest_sha256'])
                              > tuple(last_history)), 0)
                chosen = history[index]
    last_attempt = None
    def save():
        atomic_write_json(path, {'contract': CONTRACT+'.retirement-status', 'entries': states,
                                 'last_history_sort_key': last_history,
                                 'consecutive_direct_attempts': direct_streak,
                                 **({'last_attempt': last_attempt} if last_attempt is not None else {}),
                                 'observed_at_epoch': time.time()}, durable=True)
    if chosen is None:
        save()
        return False
    chosen['state'] = 'running'
    if chosen['mode'] == 'history':
        last_history = [chosen['logical_bytes'], chosen['dataset'], chosen['old_manifest_sha256']]
        direct_streak = 0
    elif chosen['mode'] == 'direct':
        direct_streak = min(MAX_CONSECUTIVE_DIRECT_RETIREMENTS, direct_streak+1)
    save()
    if on_selected is not None:
        try:
            _selected_prefetch_with_heartbeat(on_selected, chosen, save)
        except (OSError, ValueError, KeyError, TypeError, SnapshotError) as error:
            # A cache hint is never an authorization or prerequisite. Keep
            # the original child as the complete graph / lock fallback.
            print('retirement prefetch deferred: '+type(error).__name__, file=sys.stderr)
    script = ('retire_latest_fixed_generation.py' if chosen['mode'] == 'latest' else
              'deduplicate_latest_fixed_generation.py' if chosen['mode'] == 'dedup' else
              'retire_binance_archive_semantic.py' if chosen['mode'] == 'semantic' else
              'retire_superseded_repack_history.py' if chosen['mode'] == 'prepared_history'
              or chosen['mode'] == 'history' and not chosen.get('private')
              else 'retire_stream_repacked_release.py')
    command = [sys.executable, str(ROOT/'scripts'/script),
               '--dataset', chosen['dataset'], '--old-manifest-sha256', chosen['old_manifest_sha256'], '--apply']
    if chosen.get('private') is True:
        command.append('--private-source')
        if chosen['mode'] == 'history':
            command.append('--history')
    if chosen['mode'] == 'prepared_history':
        command.append('--prepared-only')
    try:
        result = run_observed(command, on_heartbeat=save, timeout=RETIRE_CHILD_TIMEOUT_SECONDS)
        if result.returncode == 0:
            value = json.loads(result.stdout)
            if (value.get('state') not in {'completed', 'no_unpinned_history'}
                    or value.get('dataset') != chosen['dataset']
                    or chosen['mode'] in {'latest', 'semantic', 'dedup'} and (
                        value.get('state') != 'completed' or value.get('apply') is not True
                        or value.get('old_manifest_sha256') != chosen['old_manifest_sha256'])
                    or chosen['mode'] == 'latest' and (
                        value.get('changed_member_values_compared') is not False
                        or value.get('old_historical_value_coverage') != 'not_compared')
                    or chosen['mode'] == 'prepared_history' and (
                        value.get('current_D_head_preserved') is not True
                        or value.get('old_manifest_sha256') != chosen['old_manifest_sha256'])):
                raise ValueError('retirement result identity differs')
            chosen.update(state='completed', deleted_bytes=value['object_bytes'],
                          deleted_objects=value.get('object_count', value.get('versions_removed', 0)),
                          estimated_reclaimed_bytes=value.get('estimated_allocated_bytes_reclaimed'))
        else:
            stderr = result.stderr[-2048:]
            reason = ('retirement_source_replication_busy' if 'source-replication-owner.lock' in stderr else
                      'retirement_backup_cycle_busy' if 'cycle.lock' in stderr else
                      'retirement_process_references' if 'running process' in stderr else
                      'retirement_current_head' if 'still a current head' in stderr else
                      'retirement_pinned' if 'pinned' in stderr else
                      'd_e_owner_busy' if any(text in stderr.lower() for text in
                          ('busy', 'lock', 'blockingioerror', 'temporarily unavailable')) else
                      'retirement_metadata_retry')
            # This timer normally fires once a minute. Retrying a blocked
            # shared-object transition every minute would win priority at
            # every tick and starve unrelated D retirement work.
            retry_delay = (180 if chosen['mode'] == 'dedup' and reason == 'd_e_owner_busy' else
                           60 if reason in {'d_e_owner_busy',
                              'retirement_source_replication_busy', 'retirement_backup_cycle_busy'} else 3600
                           if reason in {'retirement_pinned', 'retirement_current_head'} else 300)
            chosen.update(state='waiting_retry', reason=reason,
                          retry_after_epoch=time.time()+retry_delay,
                          execution_sha256=RETIRE_IMPLEMENTATION_SHA256)
            if chosen['mode'] == 'direct' and reason == 'retirement_pinned' and chosen.get('private') is not True:
                identity = handoff_completed_identity(root.parent.parent, chosen['dataset'], chosen['old_manifest_sha256'])
                if identity:
                    chosen['handoff_recheck_identity'] = identity
            if (reason == 'd_e_owner_busy' and ('retirement owner busy: publication' in stderr
                    or 'publish-retention-global.lock' in stderr)):
                chosen['owner_lock'] = 'publish_retention_global'
            print(stderr, file=sys.stderr)
    except (OSError, ValueError, KeyError, subprocess.TimeoutExpired) as error:
        chosen.update(state='waiting_retry', reason='retirement_metadata_retry', retry_after_epoch=time.time()+300,
                      execution_sha256=RETIRE_IMPLEMENTATION_SHA256)
        print(str(error), file=sys.stderr)
    last_attempt = {key: chosen[key] for key in ('dataset', 'old_manifest_sha256', 'mode', 'state',
                                                 'reason', 'owner_lock') if key in chosen}
    save()
    return True


def retirement_has_ready_work(root: Path, free_bytes: int) -> bool:
    if free_bytes >= 64 * 1024**3:
        return False
    status = json.loads((root/'retirement-queue-status.json').read_text())
    now = time.time()
    return any(item.get('state') == 'waiting' or
               (item.get('state') == 'waiting_retry' and item.get('reason') in {'d_e_owner_busy',
                'retirement_source_replication_busy', 'retirement_backup_cycle_busy'}
                and type(item.get('retry_after_epoch')) in (int, float)
                and item['retry_after_epoch'] <= now+60)
               for item in status.get('entries', []) if isinstance(item, dict))


def next_discovery_scan(now: float, retries: list[float]) -> float:
    """Rotate a blocked batch on the next timer even if its retry is later."""
    return min([now+30, *retries]) if retries else now+1


def run_prepared_history_retirement(row, *, on_heartbeat):
    """Free unpinned old versions only after this exact E source build exists."""
    stage = Path(row['source'])
    status = stage/'prepared-history-sweep.json'
    if status.is_symlink():
        raise SnapshotError('prepared history status redirected')
    if status.is_file():
        previous = json.loads(status.read_text())
        if (previous.get('contract') != 'stockagent.source_local_prepared_history.v1'
                or previous.get('dataset') != row['dataset']
                or previous.get('old_manifest_sha256') != row['expected_manifest_sha256']):
            raise SnapshotError('prepared history status identity differs')
        if previous.get('history_sweep_next_epoch', 0) > time.time():
            return previous.get('history_retirement', {})
    command = [sys.executable, str(ROOT/'scripts/retire_superseded_repack_history.py'),
               '--dataset', row['dataset'], '--old-manifest-sha256', row['expected_manifest_sha256'],
               '--prepared-only', '--apply']
    if row.get('origin') == 'private_cold':
        command.append('--private-source')
    result = run_observed(command, on_heartbeat=on_heartbeat, timeout=RETIRE_CHILD_TIMEOUT_SECONDS)
    if result.returncode:
        print(result.stderr[-2048:], file=sys.stderr)
        return {}
    summary = json.loads(result.stdout)
    if (summary.get('dataset') != row['dataset'] or summary.get('old_manifest_sha256') != row['expected_manifest_sha256']
            or summary.get('current_D_head_preserved') is not True):
        raise SnapshotError('prepared history result identity differs')
    return summary


def capacity_observation(value: dict, dataset: str, expected: str) -> dict:
    """Keep the last actual D admission figures, separate from current free space."""
    progress = value.get('progress')
    if (value.get('contract') != 'stockagent.stream_source_local_repack.v1.progress'
            or value.get('dataset') != dataset or value.get('expected_manifest_sha256') != expected
            or not isinstance(progress, dict) or progress.get('stage') != 'waiting_D_capacity'):
        return {}
    required, free, reserve = (progress.get(key) for key in
                              ('encoded_bytes_remaining', 'D_free_bytes', 'D_reserve_bytes'))
    stamp = value.get('observed_at_epoch')
    if (any(type(item) is not int or item < 0 for item in (required, free, reserve))
            or type(stamp) not in (int, float) or stamp < 0):
        return {}
    return {'D_required_bytes': required, 'D_observed_free_bytes': free, 'D_reserve_bytes': reserve,
            'D_shortfall_bytes': max(0, required+reserve-free), 'D_capacity_observed_epoch': stamp}


def prepared_source_observation(row: dict) -> dict:
    """Expose a sealed E source after any retryable post-build failure."""
    if row.get('origin') not in ('cold', 'private_cold'):
        return {}
    try:
        stage = Path(row['source'])
        if stage.is_symlink() or stage.resolve(strict=True) != stage:
            return {}
        prepared = stream_repack._read_prepared(stage, row['dataset'], row['expected_manifest_sha256'])
        return {'E_prepared': True, 'prepared_files': prepared['files'],
                'prepared_logical_bytes': prepared['logical_bytes']}
    except (OSError, ValueError, SnapshotError, KeyError, TypeError):
        return {}


def reusable_prior(previous: dict, name: str, expected: str) -> dict:
    """Reset an abandoned run without forgetting its sealed E checkpoint."""
    prior = previous.get((name, expected), {})
    if not isinstance(prior, dict) or prior.get('expected_manifest_sha256') != expected:
        return {}
    if prior.get('state') == 'running':
        if prior.get('E_prepared') is not True:
            return {}
        # This is a scheduling hint, not a new proof: the canonical stream
        # worker still validates prepared metadata and the source identity.
        # Never carry the abandoned owner, reason, or retry clock forward.
        return {key: prior[key] for key in
                ('E_prepared', 'prepared_files', 'prepared_logical_bytes', 'E_foreign_head_staged')
                if key in prior}
    return prior


def prepared_installer_busy(root: Path) -> bool:
    """Observe the existing install owner without waiting or changing its lock."""
    path = root/'prepared-install-queue-owner.lock'
    try:
        descriptor = os.open(path, os.O_RDWR | os.O_NOFOLLOW | os.O_CLOEXEC)
    except FileNotFoundError:
        return False
    try:
        info = os.fstat(descriptor)
        if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1 or not os.path.samestat(info, path.lstat()):
            raise SnapshotError('prepared install owner lock changed')
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return True
        fcntl.flock(descriptor, fcntl.LOCK_UN)
        return False
    finally:
        os.close(descriptor)


def defer_sealed_install(row: dict, prior: dict, *, installer_busy: bool, now: float):
    """Leave sealed builds to the occupied independent installer for this turn."""
    if (not installer_busy or row.get('origin') not in ('cold', 'private_cold')
            or prior.get('E_prepared') is not True or prior.get('state') != 'retrying'
            or prior.get('reason') not in {'d_e_owner_busy', 'repack_D_capacity', 'repack_temporary_failure'}):
        return None
    return {**prior, 'state': 'retrying', 'reason': 'd_e_owner_busy', 'retry_after_epoch': now+60}


_FOREIGN_HEAD_STILL_HELD = set()


def foreign_head_wait_observation(row: dict, prior: dict, cold: Path) -> str:
    """Check a sealed public generation before rerunning its foreign-head block.

    Only a still-held observation is cached for this bounded scheduler process.
    Missing/changed E evidence or unknown D metadata falls back to the original
    cooldown and child; a released head always goes through the original child.
    """
    if (row.get('origin') != 'cold' or prior.get('state') != 'retrying'
            or prior.get('reason') != 'repack_foreign_head_wait' or prior.get('E_prepared') is not True):
        return 'not_applicable'
    try:
        from scripts.run_stream_packed_repack import foreign_source_head
        name, digest = row['dataset'], row['expected_manifest_sha256']
        stage = Path(row['source'])
        if stage.is_symlink() or stage.resolve(strict=True) != stage:
            return 'unknown'
        stream_repack._read_prepared(stage, name, digest)
        key = (str(cold), name, digest)
        if key in _FOREIGN_HEAD_STILL_HELD:
            return 'held'
        if foreign_source_head(cold, name, digest, 'penguin'):
            if len(_FOREIGN_HEAD_STILL_HELD) >= 2048:
                _FOREIGN_HEAD_STILL_HELD.clear()
            _FOREIGN_HEAD_STILL_HELD.add(key)
            return 'held'
        return 'released'
    except (OSError, ValueError, TypeError, KeyError, SnapshotError):
        return 'unknown'


def load_generation_holds(path: Path = GENERATION_HOLDS) -> set[tuple[str, str]]:
    """Hold only the measured fixed generations until their packer is upgraded.

    A hold is not a claim that any data was packed, copied, or lost.  It lets
    independent queue entries proceed, and removing it automatically makes
    the exact generation eligible again without discarding its E checkpoint.
    """
    if not os.path.lexists(path):
        return set()
    if path.is_symlink() or not path.is_file() or path.stat().st_size > 256 * 1024:
        raise SnapshotError('source-local generation holds are redirected or oversized')
    value = json.loads(path.read_text())
    if (not isinstance(value, dict) or value.get('contract') != CONTRACT+'.generation-holds-v1'
            or not isinstance(value.get('entries'), list) or len(value['entries']) > 1024):
        raise SnapshotError('source-local generation holds have an invalid contract')
    result = set()
    for item in value['entries']:
        if not isinstance(item, dict):
            raise SnapshotError('invalid source-local generation hold')
        name, digest, reason = item.get('dataset'), item.get('expected_manifest_sha256'), item.get('reason')
        if (not isinstance(name, str) or re.fullmatch(r'[a-zA-Z0-9][a-zA-Z0-9._-]{0,127}', name) is None
                or not isinstance(digest, str) or re.fullmatch(r'[a-f0-9]{64}', digest) is None
                or reason != 'source_duplicate_bloat'):
            raise SnapshotError('invalid source-local generation hold')
        key = (name, digest)
        if key in result:
            raise SnapshotError('duplicate source-local generation hold')
        result.add(key)
    return result


def effective_generation_holds(workspace: Path) -> set[tuple[str, str]]:
    return load_generation_holds() | source_repack_bloat.auto_holds(workspace)


def generation_hold(row: dict, holds: set[tuple[str, str]], *, now: float):
    if (row.get('origin') not in ('cold', 'private_cold')
            or (row.get('dataset'), row.get('expected_manifest_sha256')) not in holds):
        return None
    return {'dataset': row['dataset'], 'expected_manifest_sha256': row['expected_manifest_sha256'],
            'state': 'retrying', 'reason': 'source_duplicate_bloat', 'retry_after_epoch': now+300}


def prioritize_E_only(rows: list[dict], states: list[dict], E_only: set[tuple[str, str]], *,
                      installer_busy: bool, current):
    """Do not let a different dataset consume D while its head is E-only."""
    if not E_only:
        return current
    if installer_busy:
        return None
    urgent = [row for row in rows if (row['dataset'], row['expected_manifest_sha256']) in E_only]
    if not urgent:
        return None
    selected = min(urgent, key=lambda row: row['dataset'])
    for state in states:
        if (state['dataset'], state['expected_manifest_sha256']) == (
                selected['dataset'], selected['expected_manifest_sha256']):
            state.update(state='waiting', reason='repack_E_first_install_waiting')
            state.pop('retry_after_epoch', None)
            break
    return selected


def carry_prepared_stages(rows: list[dict], previous_entries, workspace: Path,
                          E_only: set[tuple[str, str]], *, holds=frozenset()) -> list[dict]:
    """Keep recorded sealed or dedup-planned generations across D scan rotation."""
    if not isinstance(previous_entries, list) or len(previous_entries) > 1024:
        return rows
    carried = list(rows)
    # Only discoveries (not restored E-only emergency rows) suggest which
    # generation is currently selected. The worker rechecks the real head.
    discovered = {row['dataset']: row for row in rows
                  if (row['dataset'], row['expected_manifest_sha256']) not in E_only}
    for previous in previous_entries:
        if not isinstance(previous, dict):
            continue
        name, digest = previous.get('dataset'), previous.get('expected_manifest_sha256')
        if not isinstance(name, str) or not isinstance(digest, str):
            continue
        if (previous.get('origin') not in ('cold', 'private_cold')
                or previous.get('state') == 'published' or
                (previous.get('E_prepared') is not True and previous.get('E_dedup_planned') is not True
                 and (name, digest) not in holds)):
            continue
        candidate = {'dataset': name, 'expected_manifest_sha256': digest, 'origin': previous['origin'],
                     'source': str(workspace/'migration/stream-source-repack-v1'/name/digest),
                     'files': previous.get('files'), 'logical_bytes': previous.get('logical_bytes')}
        try:
            validate_entries([candidate], workspace)
        except (TypeError, ValueError, SnapshotError):
            continue  # Invalid telemetry cannot create a storage operation.
        if not any(row['dataset'] == name and row['expected_manifest_sha256'] == digest for row in carried):
            if len(carried) >= 1024:
                raise SnapshotError('too many retained E prepared generations for one queue')
            current = discovered.get(name)
            if (current is not None and current.get('origin') == candidate['origin']
                    and (name, digest) not in E_only):
                candidate['superseded_by_manifest_sha256'] = current['expected_manifest_sha256']
            carried.append(candidate)
    return carried


def main(*, on_result=None) -> int:
    config = workspace_volume.configuration()
    workspace_volume.check(c=config)
    workspace = Path(config['mount_point'])
    root = workspace/'migration/source-local-exact-repack'
    root.mkdir(mode=0o700, parents=True, exist_ok=True)
    if root.is_symlink() or root.resolve(strict=True) != root:
        raise SnapshotError('repack queue root is redirected')
    rows = load_queue(CONFIG, workspace)
    lock = os.open(root/'queue-owner.lock', os.O_CREAT | os.O_RDWR | os.O_CLOEXEC | os.O_NOFOLLOW, 0o600)
    try:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return 0
        # The independent retirement timer shares the canonical retirement
        # owner. An unrelated busy replication owner must never make this E
        # build wait through several full retirement retry windows.
        discovery_path = root/'discovered-sources.json'
        disposition = testing_disposition()
        try:
            discovery = json.loads(discovery_path.read_text()) if not discovery_path.is_symlink() else {}
            if not discovery_is_fresh(discovery, time.time(), disposition=disposition):
                raise ValueError('refresh discovery')
            found = validate_entries(discovery.get('entries'), workspace)
        except (OSError, ValueError, SnapshotError):
            cold = Path('/srv/stockagent-packed')
            catalog = _load_catalog(DEFAULT_CATALOG)
            fixed = discover_fixed_sources(workspace, cold, catalog=catalog)
            private_errors = []
            private, private_cursor, private_more = discover_private_sources(
                workspace, cold, cursor=str(discovery.get('private_cursor', '')), with_cursor=True,
                on_error=lambda error: private_errors.append(error) if len(private_errors) < 128 else None)
            private_names = {row['dataset'] for row in private}
            found = private + [row for row in fixed if row['dataset'] not in private_names]
            fixed_names = {row['dataset'] for row in found}
            found += [row for row in discover_cold_sources(workspace, cold, catalog=catalog)
                             if row['dataset'] not in fixed_names]
            others, cursor = discover_other_cold_sources(workspace, cold, catalog=catalog,
                                                        cursor=str(discovery.get('extra_cursor', '')),
                                                        disposition=disposition)
            present = {row['dataset'] for row in found}
            found = validate_entries(found + [row for row in others if row['dataset'] not in present], workspace)
            atomic_write_json(discovery_path, {'contract': DISCOVERY_CONTRACT, 'entries': found,
                'private_source_enrollment_sha256': private_source_enrollment_sha(),
                'testing_disposition_identity_sha256': disposition.get('identity_sha256') if disposition else None,
                'extra_cursor': cursor, 'extra_has_more': len(others) == 16,
                'private_cursor': private_cursor, 'private_has_more': private_more,
                'private_errors': private_errors,
                'observed_at_epoch': time.time(), 'next_scan_epoch': time.time()+900}, durable=True)
            discovery = {'extra_cursor': cursor, 'extra_has_more': len(others) == 16,
                         'private_cursor': private_cursor, 'private_has_more': private_more,
                         'private_errors': private_errors, 'next_scan_epoch': time.time()+900}
        configured = {row['dataset'] for row in rows}
        rows = validate_entries(rows + [row for row in found if row['dataset'] not in configured], workspace)
        discovered = {(row['dataset'], row['expected_manifest_sha256']) for row in rows}
        E_only = staged_head_capacity_relief.E_only_pending_generations(workspace)
        # A bounded discovery cursor may have moved past an interrupted
        # generation. Recover its exact original prepared identity directly.
        for name, digest in sorted(E_only):
            stage = workspace/'migration/stream-source-repack-v1'/name/digest
            prepared = stream_repack._prepared_metadata(stage, name, digest)
            relief = staged_head_capacity_relief.load(stage, name, digest, prepared)
            if relief is None:
                raise SnapshotError('E-only recovery journal disappeared')
            origin = 'private_cold' if relief.get('private') is True else 'cold'
            matching = [row for row in rows if row['dataset'] == name
                        and row['expected_manifest_sha256'] == digest]
            if any(row.get('origin') != origin for row in matching):
                raise SnapshotError('E-only recovery journal authority differs from queue')
            if not matching:
                rows.append({'dataset': name, 'expected_manifest_sha256': digest, 'origin': origin,
                             'source': str(stage), 'files': prepared['files'],
                             'logical_bytes': prepared['logical_bytes']})
        for planned in source_repack_bloat.auto_hold_rows(workspace):
            if len(rows) >= 960:  # Preserve room for carried in-flight generations.
                break
            name, digest = planned['dataset'], planned['expected_manifest_sha256']
            if prior_published(root, planned):
                continue
            if any(row['dataset'] == name and row['expected_manifest_sha256'] == digest for row in rows):
                continue
            # Discovery can be older than the exact head that created this
            # hold. Do not infer supersession from its cached generation; the
            # canonical publisher checks the current head before any change.
            rows.append(planned)
        state_path = root/'queue-status.json'
        try:
            old = json.loads(state_path.read_text()) if not state_path.is_symlink() else {}
        except (OSError, ValueError):
            old = {}
        if not isinstance(old, dict):
            old = {}
        old_entries = old.get('entries')
        if not isinstance(old_entries, list) or len(old_entries) > 1024:
            old_entries = []
        if old.get('contract') != CONTRACT+'.status':
            old_entries = []
        holds = effective_generation_holds(workspace)
        rows = validate_entries(carry_prepared_stages(rows, old_entries, workspace, E_only, holds=holds), workspace,
                                allow_generations=True)
        queue_identity = hashlib.sha256(json.dumps({'rows': rows, 'holds': sorted(holds),
            'private_errors': discovery.get('private_errors', [])},
            sort_keys=True, separators=(',', ':')).encode()).hexdigest()
        if (old.get('contract') == CONTRACT+'.status' and old.get('queue_identity') == queue_identity
                and isinstance(old.get('next_scan_epoch'), (int, float))
                and old['next_scan_epoch'] > time.time() and not E_only):
            return 0
        previous = {(item.get('dataset'), item.get('expected_manifest_sha256')): item for item in old_entries
                    if isinstance(item, dict) and isinstance(item.get('dataset'), str)
                    and isinstance(item.get('expected_manifest_sha256'), str)} \
            if old.get('contract') == CONTRACT+'.status' else {}
        states = []
        chosen = None
        now = time.time()
        installer_busy = prepared_installer_busy(root)
        for row in rows:
            name, expected = row['dataset'], row['expected_manifest_sha256']
            prior = reusable_prior(previous, name, expected)
            if prior_published(root, row):
                item = {'dataset': name, 'expected_manifest_sha256': expected, 'state': 'published'}
            elif (row.get('dependency_pin_hint') is True and (name, expected) not in E_only
                  and prior.get('E_prepared') is not True):
                # A positive fixed-parent hint prevents writing a duplicate
                # before the original reference owner can retire it. A missing
                # hint never authorizes deletion, and discovery refreshes it.
                item = {'dataset': name, 'expected_manifest_sha256': expected, 'state': 'retrying',
                        'reason': 'relief_pinned', 'retry_after_epoch': now+900}
            elif row.get('superseded_by_manifest_sha256') and (name, expected) not in E_only:
                # Preserve the old sealed source and work on the newly
                # discovered head. It is never installed over a newer head.
                item = {'dataset': name, 'expected_manifest_sha256': expected, 'state': 'blocked_mismatch',
                        'reason': 'source_head_advanced', 'retry_after_epoch': now+1800,
                        'superseded_by_manifest_sha256': row['superseded_by_manifest_sha256']}
            elif (name, expected) not in E_only and (deferred := generation_hold(row, holds, now=now)):
                item = deferred
            elif prior.get('reason') == 'source_head_advanced' and (name, expected) in discovered:
                # A future discovery may select this exact generation again.
                # Let the canonical worker check the current head immediately.
                item = {'dataset': name, 'expected_manifest_sha256': expected, 'state': 'waiting'}
                if chosen is None:
                    chosen = row
            elif prior.get('reason') == 'source_duplicate_bloat':
                # Removing the exact hold is the release signal, not its old
                # five-minute display clock. Never delete its E checkpoint.
                item = {'dataset': name, 'expected_manifest_sha256': expected, 'state': 'waiting'}
                if chosen is None:
                    chosen = row
            elif ((name, expected) not in E_only and
                  (head_wait := foreign_head_wait_observation(row, prior, Path('/srv/stockagent-packed')))
                  in {'held', 'released'}):
                if head_wait == 'held':
                    # Keep the original generation visible to the prepared
                    # installer without restarting its already-sealed child.
                    item = {**prior, 'state': 'retrying', 'reason': 'repack_foreign_head_wait',
                            'retry_after_epoch': now+60}
                else:
                    item = {'dataset': name, 'expected_manifest_sha256': expected, 'state': 'waiting'}
                    if chosen is None:
                        chosen = row
            elif (prior.get('state') == 'blocked_mismatch'
                  and isinstance(prior.get('retry_after_epoch'), (int, float))
                  and prior['retry_after_epoch'] > now):
                item = prior
            elif isinstance(prior.get('retry_after_epoch'), (int, float)) and prior['retry_after_epoch'] > now:
                item = prior
            elif deferred := defer_sealed_install(row, prior, installer_busy=installer_busy, now=now):
                item = deferred
            else:
                item = {'dataset': name, 'expected_manifest_sha256': expected, 'state': 'waiting'}
                if chosen is None:
                    chosen = row
            if prior.get('E_prepared') is True and item.get('state') != 'published':
                item.update(E_prepared=True,
                            prepared_files=prior.get('prepared_files'),
                            prepared_logical_bytes=prior.get('prepared_logical_bytes'))
                if prior.get('E_foreign_head_staged') is True or prior.get('reason') == 'repack_foreign_head_wait':
                    item['E_foreign_head_staged'] = True
            if prior.get('E_dedup_planned') is True and item.get('state') != 'published':
                item['E_dedup_planned'] = True
            item.update({key: row[key] for key in ('origin', 'logical_bytes', 'files') if key in row})
            states.append(item)
        chosen = prioritize_E_only(rows, states, E_only, installer_busy=installer_busy, current=chosen)
        def save(*, next_scan_epoch=None):
            atomic_write_json(state_path, {'contract': CONTRACT+'.status', 'entries': states,
                'observed_at_epoch': time.time(), 'scope': 'registered_and_bounded_other_D_heads',
                'private_discovery_errors': discovery.get('private_errors', []),
                'private_discovery_next_scan_epoch': discovery.get('next_scan_epoch'),
                'queue_identity': queue_identity,
                **({'next_scan_epoch': next_scan_epoch} if next_scan_epoch is not None else {})}, durable=True)
        if chosen is None:
            retries = [item['retry_after_epoch'] for item in states
                       if isinstance(item.get('retry_after_epoch'), (int, float)) and item['retry_after_epoch'] > now]
            if discovery.get('extra_has_more') or discovery.get('private_has_more'):
                # A bounded batch may contain only temporarily blocked large
                # datasets. Rotate past it instead of letting its cooldowns
                # indefinitely hide unrelated datasets later in the census.
                current = json.loads(discovery_path.read_text())
                next_scan = next_discovery_scan(now, retries)
                current['next_scan_epoch'] = next_scan
                atomic_write_json(discovery_path, current, durable=True)
                save(next_scan_epoch=next_scan)
            else:
                save(next_scan_epoch=min(retries, default=now+900))
            return 0
        workspace_volume.admit(0, c=config)
        item = next(value for value in states if value['dataset'] == chosen['dataset']
                    and value['expected_manifest_sha256'] == chosen['expected_manifest_sha256'])
        item['state'] = 'running'
        save()
        if chosen.get('origin') in ('cold', 'private_cold'):
            command = [sys.executable, str(ROOT/'scripts/run_stream_packed_repack.py'),
                       '--dataset', chosen['dataset'],
                       '--expected-manifest-sha256', chosen['expected_manifest_sha256'],
                       '--progress-file', str(workspace/'migration/stream-source-repack-v1/queue-progress.json')]
            if chosen.get('origin') == 'private_cold':
                command.append('--private-source')
        else:
            command = [sys.executable, str(ROOT/'scripts/repack_fixed_release_on_e.py'),
                       '--dataset', chosen['dataset'], '--source', chosen['source'],
                       '--expected-manifest-sha256', chosen['expected_manifest_sha256'],
                       '--progress-file', str(root/'queue-progress.json')]
        try:
            # A large dataset may legitimately need longer than two hours;
            # killing it would throw away its in-progress sequential build.
            result = run_observed(command, on_heartbeat=save)
            stderr = result.stderr[-2048:]
            if result.returncode == 0 and prior_published(root, chosen):
                item.update(state='published')
            elif 'duplicate source content queued for E dedup candidate' in stderr:
                item.update(state='retrying', reason='source_duplicate_bloat', E_dedup_planned=True,
                            retry_after_epoch=time.time()+300)
            elif chosen.get('origin') in ('cold', 'private_cold') and missing_cold_inventory(stderr):
                # This exact dataset lacks an inventory. Keep it visible and
                # retry later, but allow the batch to work on unrelated heads.
                item.update(state='blocked_mismatch', reason='repack_source_inventory_missing',
                            retry_after_epoch=time.time()+300)
            elif 'another publisher current head prevents old-source retirement; E prepared' in stderr:
                item.update(state='retrying', reason='repack_foreign_head_wait', retry_after_epoch=time.time()+300)
                try:
                    prepared = stream_repack._prepared_metadata(Path(chosen['source']), chosen['dataset'],
                                                                chosen['expected_manifest_sha256'])
                    item.update(E_foreign_head_staged=True, E_prepared=True, prepared_files=prepared['files'],
                                prepared_logical_bytes=prepared['logical_bytes'])
                except (OSError, ValueError, SnapshotError, KeyError) as error:
                    print('foreign-head E proof deferred: '+str(error), file=sys.stderr)
            elif any(text in stderr for text in ('source content or metadata differs',
                    'different member set', 'source file changed while packing',
                    'fixed release is no longer the current dataset head', 'original head changed',
                    'streaming repack fixed head changed')):
                item.update(state='blocked_mismatch', reason='source_or_head_mismatch',
                            retry_after_epoch=time.time()+1800)
            elif 'D needs space for this dataset' in stderr or 'D reserve reached' in stderr:
                item.update(state='retrying', reason='repack_D_capacity', retry_after_epoch=time.time()+300)
                if chosen.get('origin') in ('cold', 'private_cold'):
                    try:
                        progress_path = workspace/'migration/stream-source-repack-v1/queue-progress.json'
                        if not progress_path.is_symlink() and progress_path.is_file():
                            item.update(capacity_observation(json.loads(progress_path.read_text()),
                                                            chosen['dataset'], chosen['expected_manifest_sha256']))
                        prepared = stream_repack._prepared_metadata(Path(chosen['source']), chosen['dataset'],
                                                                    chosen['expected_manifest_sha256'])
                        item.update(E_prepared=True, prepared_files=prepared['files'],
                                    prepared_logical_bytes=prepared['logical_bytes'])
                        # The separate retirement timer sees the sealed source
                        # in this status after save(). It takes the very same
                        # retirement owner and does not hold up the next build.
                        item['prepared_history_queued'] = True
                    except (OSError, ValueError, SnapshotError, KeyError, subprocess.TimeoutExpired) as error:
                        print('prepared history deferred: '+str(error), file=sys.stderr)
            elif 'day-trade priority window' in stderr.lower():
                item.update(state='retrying', reason='tw_day_trade_priority_window', retry_after_epoch=time.time()+60)
            elif any(text in stderr.lower() for text in ('no space left', 'capacity reserve', 'insufficient capacity')):
                item.update(state='retrying', reason='workspace_capacity', retry_after_epoch=time.time()+300)
            elif any(text in stderr.lower() for text in ('checksum mismatch', 'sha-256 mismatch', 'digest mismatch')):
                item.update(state='blocked_mismatch', reason='repack_source_integrity',
                            retry_after_epoch=time.time()+1800)
            elif any(name in stderr for name in ('source-local-retirement-turn.lock', 'source-local-retirement-intent.lock')):
                item.update(state='retrying', reason='repack_retirement_turn', retry_after_epoch=time.time()+30)
            elif 'lock' in stderr.lower() or 'busy' in stderr.lower():
                item.update(state='retrying', reason='d_e_owner_busy', retry_after_epoch=time.time()+60)
                if dataset_publication_owner_busy(stderr, chosen['dataset']):
                    item['owner_lock'] = 'dataset_publication'
            else:
                item.update(state='retrying', reason='repack_temporary_failure', retry_after_epoch=time.time()+300)
            if item.get('state') == 'retrying':
                item.update(prepared_source_observation(chosen))
            if result.returncode:
                print(stderr, file=sys.stderr)
        except OSError as error:
            item.update(state='retrying', reason='repack_temporary_failure', retry_after_epoch=time.time()+300)
            print(str(error), file=sys.stderr)
        save()
        # Notify the outer scheduler only after the individual state and its
        # fixed publication receipt have been recorded. A busy owner, an idle
        # queue, or a retry never counts as forward progress.
        if on_result is not None:
            on_result('source_inventory_missing' if item.get('reason') == 'repack_source_inventory_missing'
                      else 'duplicate_preflight' if item.get('reason') == 'source_duplicate_bloat'
                      else 'foreign_head_staged' if item.get('reason') == 'repack_foreign_head_wait'
                      else 'dataset_owner_busy' if not E_only and item.get('owner_lock') == 'dataset_publication'
                      else item['state'])
        return 0
    finally:
        os.close(lock)


def run_ready_batch(*, run=main, guard=lambda: None, monotonic=time.monotonic,
                    max_items=4, max_seconds=180):
    """Continue past independent blockers and completed publications.

    Each iteration releases the original queue and publication locks before
    the next. The time bound limits starting further work, not an in-flight
    dataset, whose resumable journal and trading-window timeout remain in force.
    """
    if (type(max_items) is not int or max_items <= 0 or
            type(max_seconds) not in (int, float) or not math.isfinite(max_seconds) or max_seconds <= 0):
        raise ValueError('invalid source-local repack batch limits')
    started = monotonic()
    attempted = published = 0
    reason = 'batch_limit'
    while attempted < max_items and (not attempted or monotonic()-started < max_seconds):
        try:
            guard()
        except SnapshotError as error:
            if 'day-trade priority window' not in str(error).lower():
                raise
            reason = 'trading_priority'
            break
        outcome = []
        code = run(on_result=outcome.append)
        attempted += 1
        if code != 0:
            return {'attempted': attempted, 'published': published, 'reason': 'owner_error', 'exit_code': code}
        if outcome in (['source_inventory_missing'], ['duplicate_preflight'], ['foreign_head_staged'], ['dataset_owner_busy']):
            continue
        if outcome != ['published']:
            reason = 'no_publication'
            break
        published += 1
    else:
        reason = 'batch_limit' if attempted >= max_items else 'time_limit'
    return {'attempted': attempted, 'published': published, 'reason': reason, 'exit_code': 0}


if __name__ == '__main__':
    try:
        from scripts.run_bounded_storage_maintenance import TradingPriorityGuard
        result = run_ready_batch(guard=TradingPriorityGuard().check)
        print({'source_local_repack_batch': result}, flush=True)
        raise SystemExit(result['exit_code'])
    except (OSError, ValueError, SnapshotError) as error:
        print(f'{type(error).__name__}: {error}', file=sys.stderr)
        raise SystemExit(1)
