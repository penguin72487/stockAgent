#!/usr/bin/env python3
"""Resume explicitly enrolled C-to-E copies through the original copy owner.

This queue only stages and verifies source generations. It does not switch
readers, delete source names, publish to D, or claim NAS acceptance.
"""
from __future__ import annotations

import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import shlex
import subprocess
import sys
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import ExitStack

from downloader.artifact_io import atomic_write_json
from scripts import stage_futures_e_working_copy as stage
from scripts import publish_data_releases as publisher
from stockagent.data_sync.completed_forward_capacity import CapacityDeferred
from stockagent.data_sync.desync_snapshots import SnapshotError
from stockagent.data_sync import workspace_relocation as relocation
from stockagent.data_sync import workspace_volume as volume

CONTRACT = 'stockagent.workspace_source_staging_queue.v1'
ROOT = Path(__file__).resolve().parents[1]
CONFIG = ROOT / 'configs/data_sync/workspace_source_staging.json'

# Only these fixed source generations use the user-requested single-pass
# placement contract. This never enrolls the current live ledger.
OFFLINE_REPAIR_SCOPE = ('tw-daytrade-atomic-repair-e-20261108-v1',
    'service_data/live/tw_day_trade_simulation.atomic-repair-fp8q78y4', 'promotion_receipt.json')
OFFLINE_ROLLBACK_SCOPE = ('tw-daytrade-close-rollback-e-20261108-v1',
    'service_data/live/official-close-settlement-kb2xdiof', 'receipt.json')
OFFLINE_REPAIR_SCOPES = frozenset({OFFLINE_REPAIR_SCOPE, OFFLINE_ROLLBACK_SCOPE})
BYBIT_STREAM_SCOPE = ('bybit-source-stream-e-20261108-v1',
    'service_data/stockagent-live/data_bybit', 'download_summary.json')
OPTIONS_STREAM_SCOPE = ('tw-index-options-daily-stream-e-20261108-v1',
    'data_tw_index_options_daily', 'manifest.json')
OPTIONS_RAW_STREAM_SCOPE = ('tw-index-options-raw-stream-e-20261108-v1',
    'data_tw_index_options_daily/raw', 'annual/2001_opt.zip')
STREAM_SCOPES = {
    ('tw-minute-train-stream-e-20261108-v1', 'data_tw_minute/research_dataset', 'manifest.json'),
    ('tw-minute-source-stream-e-20261108-v1', 'data_tw_minute/shioaji_1m', 'download_summary.json'),
    BYBIT_STREAM_SCOPE,
    OPTIONS_STREAM_SCOPE,
    OPTIONS_RAW_STREAM_SCOPE,
    *OFFLINE_REPAIR_SCOPES,
}
MINUTE_SOURCE_GENERATIONS = frozenset({
    'tw-minute-source-e-primary-20261104-v1', 'tw-minute-source-stream-e-20261108-v1',
})
OPTIONS_SOURCE_GENERATIONS = frozenset({
    'tw-index-options-daily-e-primary-20261104-v1', 'tw-index-options-daily-stream-e-20261108-v1',
})
OFFLINE_REPAIR_RECEIPT = Path('/srv/stockagent-workspace/verify/tw-provisional-decisions-20261104-Qxezwm5I/scoped-batch-candidate.json')
OFFLINE_REPAIR_RECEIPT_SHA256 = '6ec73d784fa4f2a20fc492b379858e9db428620a5ea580f07471b1b948418902'
OFFLINE_ROLLBACK_RECEIPT_SHA256 = 'b07f12be4d088075b528f187c0ef9801b7e42d229a4d01e7a1d34c6847c984c5'


def offline_repair_receipt_sha(item: dict) -> str:
    scope = stream_scope(item)
    if scope == OFFLINE_REPAIR_SCOPE:
        return OFFLINE_REPAIR_RECEIPT_SHA256
    if scope == OFFLINE_ROLLBACK_SCOPE:
        return OFFLINE_ROLLBACK_RECEIPT_SHA256
    raise ValueError('service source is outside the fixed offline repair scopes')


def stream_scope(item: dict) -> tuple:
    return item.get('name'), item.get('source'), item.get('marker')


def check_offline_repair_provenance(item: dict) -> None:
    """Bind each explicitly named offline source to its immutable receipt."""
    scope = stream_scope(item)
    expected_sha = offline_repair_receipt_sha(item)
    path = (OFFLINE_REPAIR_RECEIPT if scope == OFFLINE_REPAIR_SCOPE else
            ROOT/'service_data/live/tw_day_trade_simulation/settlement_receipts/2026-10-29-1b2d5096974bb6ab.json')
    if path.is_symlink() or path.stat().st_size > 2 * 1024**2:
        raise ValueError('offline repair receipt is redirected or oversized')
    raw = path.read_bytes()
    value = json.loads(raw)
    if hashlib.sha256(raw).hexdigest() != expected_sha or not isinstance(value, dict):
        raise ValueError('offline repair receipt identity or scope differs')
    if scope == OFFLINE_REPAIR_SCOPE:
        valid = (value.get('candidate_directory') == str(ROOT / scope[1])
                 and value.get('status') == 'candidate_built_not_promoted'
                 and value.get('publication') == 'not_attempted'
                 and value.get('simulation_only') is True)
    else:
        marker = ROOT/scope[1]/scope[2]
        valid = (not marker.is_symlink() and marker.stat().st_size <= 2*1024**2
                 and hashlib.sha256(marker.read_bytes()).hexdigest() == expected_sha
                 and value.get('contract') == 'user_authorized_official_close_paper_settlement_v1'
                 and value.get('status') == 'applied' and value.get('simulation_only') is True
                 and value.get('production_order_possible') is False
                 and value.get('operation_id') == '2026-10-29:1b2d5096974bb6ab'
                 and value.get('rollback_directory') == str(ROOT/'artifacts/live/official-close-settlement-kb2xdiof/ledger'))
    if not valid:
        raise ValueError('offline repair receipt identity or scope differs')

# These exact collectors take this existing root lock around their complete
# write. Do not discover arbitrary lock names or broaden this by directory.
ROOT_WRITER_LOCKS = {
    'coinmetrics-community': 'data_coinmetrics_community',
    'crypto-reference': 'data_crypto_reference',
    'crypto-historical-public': 'data_crypto_historical_public',
    'dune-crypto': 'data_dune_crypto',
    'crypto-etf-history': 'data_crypto_etf',
    'keyed-public-catalogs': 'data_keyed_public_catalogs',
    'forex-frankfurter': 'data_forex_frankfurter',
    'binance-public-archive': 'data_binance_archive',
    'free-public-context': 'data_free_public',
    'public-economic-history': 'data_public_economic',
}


def original_source_owners(entry: dict) -> list[dict]:
    """Include catalog, publisher-native and explicitly reviewed writer locks."""
    owners = [entry, *publisher._native_source_lock_entries(entry)]
    root = ROOT_WRITER_LOCKS.get(entry.get('dataset'))
    if root is not None and entry.get('source') == root:
        owners.append({**entry, 'source': str(ROOT / root / 'source-owner-scope'),
                       'source_coordination_lock_scope': 'source_parent',
                       'source_coordination_lock': '.download.lock',
                       'source_coordination_lock_mode': 'exclusive'})
    # The official economic collector has independent writers for these three
    # providers; fence every original lock, not a new unrelated global lock.
    if entry.get('dataset') == 'public-economic-history' and entry.get('source') == 'data_public_economic':
        for provider in ('bea', 'census', 'moi'):
            owners.append({**entry, 'source': str(ROOT / 'data_public_economic/source-owner-scope'),
                           'source_coordination_lock_scope': 'source_parent',
                           'source_coordination_lock': f'.{provider}.lock',
                           'source_coordination_lock_mode': 'exclusive'})
    if entry.get('dataset') == 'tw-index-options-daily' and entry.get('source') == 'data_tw_index_options_daily':
        owners.append({**entry, 'source': str(ROOT / 'data_tw_index_options_daily/state/source-owner-scope'),
                       'source_coordination_lock_scope': 'source_parent',
                       'source_coordination_lock': 'registered_auxiliary_daily.lock',
                       'source_coordination_lock_mode': 'exclusive'})
    return owners


def _relative(value: object) -> Path:
    if not isinstance(value, str) or not value:
        raise ValueError('a nonempty relative path is required')
    path = Path(value)
    if path.is_absolute() or '..' in path.parts or path.as_posix() != value:
        raise ValueError('source and marker must be normalized relative paths')
    return path


def load_sources(path: Path = CONFIG) -> list[dict]:
    if path.is_symlink() or path.stat().st_size > 64 * 1024:
        raise ValueError('queue configuration is redirected or too large')
    value = json.loads(path.read_bytes())
    sources = value.get('sources') if isinstance(value, dict) else None
    if (not isinstance(value, dict) or value.get('schema_version') != 1
            or value.get('contract') != CONTRACT or not isinstance(sources, list)
            or len(sources) > 64):
        raise ValueError('invalid staging queue configuration')
    seen_names, seen_sources = set(), {}
    for item in sources:
        required = {'name', 'source', 'marker', 'budget_bytes', 'seed_hardlinks'}
        optional = {'catalog_source', 'preserve_symlinks', 'copy_mode'}
        if (not isinstance(item, dict) or not required <= set(item)
                or set(item) - required - optional):
            raise ValueError('invalid source staging entry')
        name, source, marker = item['name'], _relative(item['source']), _relative(item['marker'])
        catalog_source = _relative(item.get('catalog_source', item['source']))
        bybit_alias = stream_scope(item) == BYBIT_STREAM_SCOPE and catalog_source == Path('data_bybit')
        if (not isinstance(name, str) or re.fullmatch(r'[a-z0-9][a-z0-9-]{0,99}', name) is None
                or (not source.parts[0].startswith('data_') and stream_scope(item) not in OFFLINE_REPAIR_SCOPES
                    and not bybit_alias)
                or not marker.parts
                or not (source == catalog_source or source.is_relative_to(catalog_source) or bybit_alias)
                or type(item['budget_bytes']) is not int or not 0 < item['budget_bytes'] <= 1024**4
                or type(item['seed_hardlinks']) is not bool
                or type(item.get('preserve_symlinks', False)) is not bool
                or ('copy_mode' in item and (item['copy_mode'] != 'source_stream_no_readback'
                    or stream_scope(item) not in STREAM_SCOPES or item['seed_hardlinks']
                    or item.get('preserve_symlinks', False)))
                or (stream_scope(item) in OFFLINE_REPAIR_SCOPES
                    and item.get('copy_mode') != 'source_stream_no_readback')
                or (bybit_alias and item.get('copy_mode') != 'source_stream_no_readback')
                or name in seen_names or any(
                    (source == other or source.is_relative_to(other) or other.is_relative_to(source))
                    and not (source == other == Path('data_tw_minute/shioaji_1m')
                             and {name, other_name} == MINUTE_SOURCE_GENERATIONS)
                    and not (source == other == Path('data_tw_index_options_daily')
                             and {name, other_name} == OPTIONS_SOURCE_GENERATIONS)
                    and not ({source, other} == {Path('data_tw_index_options_daily'),
                                                  Path('data_tw_index_options_daily/raw')}
                             and {name, other_name} & OPTIONS_SOURCE_GENERATIONS
                             and OPTIONS_RAW_STREAM_SCOPE[0] in {name, other_name})
                    for other, other_name in seen_sources.items())):
            raise ValueError('staging entry exceeds its safe source scope')
        seen_names.add(name)
        seen_sources[source] = name
    return sources


def paths(item: dict, workspace: Path) -> tuple[Path, Path, Path, Path]:
    from scripts import project_futures_725_workspace_index as projected
    scope = next((scope for scope in (projected.default_scope(), projected.FUTURES_765)
                  if item.get('name') == scope.name), None)
    if scope is not None:
        if item != projected.handoff_item(scope):
            raise ValueError('projected futures subtree scope differs')
        source = ROOT / _relative(item['source'])
        destination = workspace / 'downloads' / projected.PARENT / _relative(item['source'])
        return (source, destination, workspace/'migration'/scope.name,
                workspace/'verify'/(scope.name+'.json'))
    source = ROOT / _relative(item['source'])
    destination = workspace / 'downloads' / item['name'] / _relative(item['source'])
    evidence = workspace / 'migration' / item['name']
    report = workspace / 'verify' / (item['name'] + '.json')
    return source, destination, evidence, report


def _read_json(path: Path) -> dict:
    if path.is_symlink() or path.stat().st_size > 128 * 1024:
        raise ValueError('staging evidence is redirected or too large')
    value = json.loads(path.read_bytes())
    if not isinstance(value, dict):
        raise ValueError('staging evidence must be an object')
    return value


def verified_report(item: dict, workspace: Path) -> dict | None:
    from scripts import project_futures_725_workspace_index as projected
    scope = next((scope for scope in (projected.default_scope(), projected.FUTURES_765)
                  if item.get('name') == scope.name), None)
    if scope is not None:
        paths(item, workspace)  # reject any widened or redirected enrollment
        return projected.validated_projection(workspace, scope=scope)
    source, destination, evidence, report = paths(item, workspace)
    if not report.exists():
        return None
    value = _read_json(report)
    copy_path = evidence / 'copy-result.json'
    if (destination.is_symlink() or not destination.is_dir()
            or destination.resolve(strict=True) != destination
            or evidence.is_symlink() or evidence.resolve(strict=True) != evidence):
        raise ValueError('completed staging paths are missing or redirected')
    copy = _read_json(copy_path)
    identity = value.get('relocation_evidence_sha256')
    fast = item.get('copy_mode') == 'source_stream_no_readback'
    if fast and stream_scope(item) not in STREAM_SCOPES:
        raise ValueError('source-stream proof is outside its explicitly enrolled scope')
    valid_copy = (copy.get('contract') == 'stockagent.source_stream_staging.v1'
                  and copy.get('state') == 'source_sha256_streamed_no_destination_readback'
                  and copy.get('source_sha256_streamed') is True
                  and copy.get('destination_independent_readback') is False
                  and copy.get('all_source_sha256_and_destination_readback_verified') is False
                  and value.get('state') == 'source_stream_staged_without_destination_readback'
                  and value.get('destination_independent_readback') is False) if fast else (
                  value.get('state') == 'verified_temporary_e_source_copy'
                  and copy.get('state') == 'exact_destination_readback_verified'
                  and copy.get('all_source_sha256_and_destination_readback_verified') is True)
    if (not valid_copy or value.get('source') != str(source) or value.get('e_destination') != str(destination)
            or value.get('relocation_evidence') != str(copy_path)
            or not isinstance(identity, str) or re.fullmatch(r'[0-9a-f]{64}', identity) is None
            or hashlib.sha256(copy_path.read_bytes()).hexdigest() != identity
            or copy.get('source_files_deleted') != 0 or value.get('source_files_deleted') != 0
            or value.get('source_alias_changed') is not False
            or value.get('files') != copy.get('files') or value.get('logical_bytes') != copy.get('logical_bytes')):
        raise ValueError('completed staging report does not match its copy receipt')
    return value


def preserve_interrupted_index(item: dict, workspace: Path, lock_root: Path) -> bool:
    """Preserve a partial inventory only if no destination data was created.

    The same nonblocking lock is held by the existing copier. No in-progress
    or indexed copy, source, destination payload, or previous proof is removed.
    """
    source, destination, evidence, _ = paths(item, workspace)
    intent_path = evidence / 'intent.json'
    if not intent_path.exists():
        return False
    with (lock_root / (item['name'] + '.lock')).open('a') as owner:
        fcntl.flock(owner, fcntl.LOCK_EX | fcntl.LOCK_NB)
        intent = _read_json(intent_path)
        if intent.get('state') != 'indexing':
            return False
        preserve_links = item.get('preserve_symlinks', False)
        expected_contract = 'workspace_scoped_relocation_v1' if preserve_links else 'workspace_exact_relocation_v1'
        if (evidence.is_symlink() or evidence.resolve(strict=True) != evidence
                or intent.get('contract') != expected_contract
                or (preserve_links and (intent.get('preserve_symlinks') is not True
                                        or intent.get('excluded_subtrees') != []))
                or intent.get('source') != str(source) or intent.get('destination') != str(destination)
                or destination.exists() or destination.is_symlink()):
            raise ValueError('partial inventory cannot be safely replanned')
        preserved = evidence.with_name(evidence.name + '.interrupted-index-' + uuid.uuid4().hex)
        os.rename(evidence, preserved)
        relocation.sync_filesystem(evidence.parent)
        return True


def _argv_mentions_source(source: Path, cwd: Path, command: str) -> bool:
    """Detect source paths in argv, including relative paths and --key=value.

    An open-FD scan alone misses an idle streaming process between writes.
    This is another observation, not a writer lease or retirement permission.
    """
    try:
        argv = shlex.split(command)
    except ValueError as error:
        raise ValueError('cannot parse a live process command') from error
    source = Path(os.path.normpath(str(source)))
    for arg in argv:
        candidate = arg.split('=', 1)[1] if arg.startswith('--') and '=' in arg else arg
        if not candidate or candidate.startswith('-'):
            continue
        path = Path(candidate)
        if not path.is_absolute():
            path = cwd / path
        path = Path(os.path.normpath(str(path)))
        if path == source or path.is_relative_to(source):
            return True
    return False


def _known_option_settlement_reader(source: Path, cwd: Path, command: str, cgroup: str) -> bool:
    """Only the registered FOP capture's exact settlement input is read-only.

    It reads this Parquet with Polars; its output and state are separate roots.
    Unknown processes and every other source argument still fail closed.
    """
    if (source != ROOT / 'data_tw_index_options_daily' or cwd != ROOT
            or cgroup.strip() != '0::/system.slice/stockagent-shioaji-taifex-bidask.service'):
        return False
    try:
        argv = shlex.split(command)
    except ValueError:
        return False
    if (len(argv) < 5 or not Path(argv[0]).name.startswith('python')
            or argv[1:3] != ['-m', 'downloader.stream_shioaji_taifex_bidask']
            or argv.count('--final-settlement-path') != 1):
        return False
    index = argv.index('--final-settlement-path') + 1
    if index >= len(argv):
        return False
    mentioned = []
    for position, arg in enumerate(argv):
        candidate = arg.split('=', 1)[1] if arg.startswith('--') and '=' in arg else arg
        if not candidate or candidate.startswith('-'):
            continue
        path = Path(candidate)
        if not path.is_absolute():
            path = cwd / path
        path = Path(os.path.normpath(str(path)))
        if path == source or path.is_relative_to(source):
            mentioned.append((position, path))
    return mentioned == [(index, source / 'txo_final_settlement_history.parquet')]


def catalog_entries(item: dict) -> list[dict]:
    catalog = _read_json(ROOT / 'configs/data_sync/packed_datasets.json')
    if not isinstance(catalog.get('datasets'), list):
        raise ValueError('invalid source catalog')
    source = _relative(item['source'])
    catalog_source = _relative(item.get('catalog_source', item['source']))
    bybit_alias = stream_scope(item) == BYBIT_STREAM_SCOPE and catalog_source == Path('data_bybit')
    if not (source == catalog_source or source.is_relative_to(catalog_source) or bybit_alias):
        raise ValueError('staging scope is outside its catalogued source')
    if bybit_alias:
        alias, physical = ROOT/catalog_source, ROOT/source
        if (not alias.is_symlink() or physical.is_symlink() or not physical.is_dir()
                or physical.resolve(strict=True) != physical or alias.resolve(strict=True) != physical):
            raise ValueError('Bybit catalog alias no longer identifies its enrolled C workspace')
    entries = [entry for entry in catalog['datasets']
               if isinstance(entry, dict) and entry.get('source') == str(catalog_source)]
    if not entries:
        raise ValueError('staging source is not an exact catalogued dataset root')
    if source != catalog_source and not bybit_alias:
        child = source.relative_to(catalog_source)
        for entry in entries:
            excluded = entry.get('excluded_subtrees', [])
            if entry.get('included_root_files') or not isinstance(excluded, list):
                raise ValueError('catalog does not cover the selected source subtree')
            for raw in excluded:
                excluded_path = _relative(raw)
                if (child == excluded_path or child.is_relative_to(excluded_path)
                        or excluded_path.is_relative_to(child)):
                    raise ValueError('selected source intersects a catalog exclusion')
    # A local exact copy may include separately catalogued descendants even
    # when their publication rights differ. Their original writer locks and
    # process blockers still have to fence the whole selected source.
    for entry in catalog['datasets']:
        if not isinstance(entry, dict) or not isinstance(entry.get('source'), str):
            continue
        try:
            nested = _relative(entry['source'])
        except ValueError:
            continue
        if (nested == source or nested.is_relative_to(source)) and entry not in entries:
            entries.append(entry)
    return entries


def check_known_running_owners(item: dict, entries: list[dict] | None = None) -> None:
    """Fail closed on catalogued writers or processes naming this source."""
    entries = catalog_entries(item) if entries is None else entries
    commands = publisher._running_commands()
    if any(publisher._blockers(entry, commands) for entry in entries):
        raise RuntimeError('the source has a live owner; wait without changing it')
    if stream_scope(item) == BYBIT_STREAM_SCOPE and publisher._blockers({
            'active_process_substrings': ['repair_bybit_1m_gaps.py', 'queue_source_anomaly_priorities.py',
                'materialize_ohlcv_daily.py', 'download_bybit_perp_15m.py', 'write_data_readmes.py'],
    }, commands):
        raise RuntimeError('the source has a live owner; wait without changing it')
    if stream_scope(item) in {OPTIONS_STREAM_SCOPE, OPTIONS_RAW_STREAM_SCOPE} and publisher._blockers({
            'active_process_substrings': ['download_taifex_final_settlement_history.py',
                'run_taifex_auxiliary_daily.sh', 'publish_txo_friday_20261023_notice.py',
                'publish_taifex_index_option_settlements.py', 'publish_taifex_gold_option_settlements.py',
                'publish_taifex_fx_option_settlements.py', 'write_data_readmes.py'],
    }, commands):
        raise RuntimeError('the source has a live owner; wait without changing it')
    source = ROOT / _relative(item['source'])
    aliases = (source, ROOT/'data_bybit') if stream_scope(item) == BYBIT_STREAM_SCOPE else (source,)
    for pid, command in commands:
        try:
            cwd = Path(os.readlink(f'/proc/{pid}/cwd'))
        except FileNotFoundError:
            continue  # process exited after the bounded command observation
        except OSError as error:
            raise ValueError('cannot inspect a live process working directory') from error
        if any(_argv_mentions_source(alias, cwd, command) for alias in aliases):
            try:
                cgroup = Path(f'/proc/{pid}/cgroup').read_text()
                current = shlex.join([os.fsdecode(arg) for arg in Path(f'/proc/{pid}/cmdline').read_bytes().split(b'\0') if arg])
            except (OSError, ValueError):
                raise RuntimeError('the source has a live owner; wait without changing it')
            if current == command and _known_option_settlement_reader(source, cwd, command, cgroup):
                continue
            raise RuntimeError('the source has a live owner; wait without changing it')


def run_source(item: dict, workspace: Path, lock_root: Path) -> dict:
    source, destination, evidence, report = paths(item, workspace)
    result = verified_report(item, workspace)
    fixed_offline_alias = (stream_scope(item) in OFFLINE_REPAIR_SCOPES and result is not None
                           and source.is_symlink() and source.resolve(strict=True) == destination)
    if not fixed_offline_alias and (source.is_symlink() or source.resolve(strict=True) != source):
        raise ValueError('source is redirected or unavailable')
    fast = item.get('copy_mode') == 'source_stream_no_readback'
    if result is not None:
        return {'name': item['name'], 'state': 'source_stream_staged' if fast else 'copy_verified', 'files': result['files'],
                'logical_bytes': result['logical_bytes'], 'proof_observed_at_epoch': result['observed_at_epoch'],
                'proof_scope': ('source_stream_sha256_no_destination_readback' if fast
                                else 'historical_complete_readback_not_current_integrity')}
    if stream_scope(item) in OFFLINE_REPAIR_SCOPES:
        check_offline_repair_provenance(item)
        entries = []  # This fixed offline candidate is not a provider dataset.
    else:
        entries = catalog_entries(item)
    with ExitStack() as locks:
        # Preserve the publisher's cooperative writer fence whenever the
        # source catalog provides one. No new provider-specific owner is made.
        registered = {}
        for entry in entries:
            # The publisher also knows native per-grain CEX writer locks;
            # looking only at source_coordination_lock misses those locks.
            for owner in original_source_owners(entry):
                configured = owner.get('source_coordination_lock')
                if not configured:
                    continue
                scope = owner.get('source_coordination_lock_scope', 'repo')
                if scope == 'repo':
                    root = publisher.REPO_ROOT
                elif scope == 'source_parent':
                    root = publisher._source_path(owner).resolve().parent
                else:
                    raise ValueError('invalid catalog source lock scope')
                # Deduplicate the actual pathname, not the scope label: the
                # same owner may be named by multiple catalog entries.
                key = str(root.absolute() / configured)
                mode = owner.get('source_coordination_lock_mode', 'exclusive')
                if key in registered and registered[key][0] != mode:
                    raise ValueError('catalog source lock modes disagree')
                registered.setdefault(key, (mode, owner))
        for key in sorted(registered):
            locks.enter_context(publisher._source_coordination_lock(registered[key][1], wait_seconds=0))
        check_known_running_owners(item, entries)
        preserve_interrupted_index(item, workspace, lock_root)
        stage.main(source=source, name=item['name'], marker=_relative(item['marker']), report=report,
                   budget=item['budget_bytes'], seed_hardlinks=item['seed_hardlinks'],
                   preserve_symlinks=item.get('preserve_symlinks', False),
                   **({'source_stream_no_readback': True} if fast else {}))
    result = verified_report(item, workspace)
    if result is None:
        raise ValueError('copy owner returned without a verified report')
    return {'name': item['name'], 'state': 'source_stream_staged' if fast else 'copy_verified', 'files': result['files'],
            'logical_bytes': result['logical_bytes'], 'proof_observed_at_epoch': result['observed_at_epoch'],
            'proof_scope': ('source_stream_sha256_no_destination_readback' if fast
                            else 'historical_complete_readback_not_current_integrity')}


def classify_error(error: Exception) -> tuple[str, str]:
    if isinstance(error, publisher.SourceCoordinationLockTimeout):
        return 'waiting', 'original_owner_or_reader'
    if isinstance(error, BlockingIOError):
        if str(error) == 'tw_day_trade_priority_window':
            return 'waiting', 'tw_day_trade_priority_window'
        return 'waiting', 'original_owner_or_reader'
    if isinstance(error, CapacityDeferred):
        reason = error.details.get('reason')
        if reason in {'namespace_lock_busy', 'filesystem_headroom_exhausted',
                      'capacity_exhausted', 'capacity_policy_changed_while_busy'}:
            return 'waiting', 'workspace_capacity_or_reservation'
        return 'blocked', 'workspace_reservation_evidence_invalid'
    if isinstance(error, subprocess.TimeoutExpired):
        command = error.cmd
        if isinstance(command, (list, tuple)) and command and command[0] == volume.POWERSHELL:
            return 'waiting', 'workspace_native_bridge_unavailable'
        return 'blocked', 'source_or_copy_recheck_required'
    if isinstance(error, SnapshotError):
        if str(error) == 'E workspace lacks physical/guest verification headroom':
            return 'waiting', 'workspace_capacity'
        if (str(error).startswith('native E workspace operation failed:')
                and 'UtilAcceptVsock' in str(error) and 'accept4 failed 110' in str(error)):
            return 'waiting', 'workspace_native_bridge_unavailable'
        return 'blocked', 'workspace_identity_or_source_invalid'
    if isinstance(error, RuntimeError) and str(error) == 'the source has a live owner; wait without changing it':
        return 'waiting', 'original_owner_or_reader'
    return 'blocked', 'source_or_copy_recheck_required'


def _auto_enroll_fixed_source(results: list[dict], name: str, *, start_allowed=None, enroll=None) -> dict:
    """Enroll a reviewed fixed E generation; its original handoff owner switches it."""
    from stockagent.data_sync import workspace_source_handoff as handoff
    current = next((row for row in results if row.get('name') == name), None)
    if current is None or current.get('state') != 'source_stream_staged':
        return {'state': 'waiting', 'reason': 'source_stream_not_staged'}
    if name in handoff.registered_names():
        return {'state': 'enrolled'}
    allowed = start_allowed or (lambda: handoff.binance_handoff_window_allowed(for_start=True))
    try:
        eligible = allowed()
    except (OSError, ValueError, TypeError, SnapshotError) as error:
        print(f'{name} handoff window: {type(error).__name__}', file=sys.stderr, flush=True)
        return {'state': 'waiting', 'reason': 'tw_day_trade_priority_window'}
    if eligible is not True:
        return {'state': 'waiting', 'reason': 'tw_day_trade_priority_window'}
    if enroll is None:
        from scripts.enroll_fixed_source_handoff import enroll
    try:
        result = enroll(name, apply=True)
        if not isinstance(result, dict) or result.get('state') not in {'already_enrolled', 'enrolled_for_existing_owner'}:
            raise ValueError('source enrollment returned an unexpected state')
        return {'state': 'enrolled'}
    except BlockingIOError:
        return {'state': 'waiting', 'reason': 'original_owner_or_reader'}
    except (OSError, ValueError, TypeError, KeyError, SnapshotError) as error:
        print(f'{name} handoff enrollment: {type(error).__name__}: {error}', file=sys.stderr, flush=True)
        return {'state': 'blocked', 'reason': 'source_or_copy_recheck_required',
                'error_type': type(error).__name__}


def auto_enroll_minute_raw(results: list[dict], *, start_allowed=None, enroll=None) -> dict:
    from stockagent.data_sync import workspace_source_handoff as handoff
    return _auto_enroll_fixed_source(results, handoff.MINUTE_RAW_NAME, start_allowed=start_allowed, enroll=enroll)


def auto_enroll_bybit(results: list[dict], *, start_allowed=None, enroll=None) -> dict:
    from stockagent.data_sync import workspace_source_handoff as handoff
    return _auto_enroll_fixed_source(results, handoff.BYBIT_NAME, start_allowed=start_allowed, enroll=enroll)


def auto_enroll_options_raw(results: list[dict], *, start_allowed=None, enroll=None) -> dict:
    from stockagent.data_sync import workspace_source_handoff as handoff
    if start_allowed is None:
        start_allowed = lambda: (handoff.binance_handoff_window_allowed(for_start=True)
                                 and handoff.options_raw_handoff_window_allowed(for_start=True))
    return _auto_enroll_fixed_source(results, handoff.OPTIONS_RAW_NAME, start_allowed=start_allowed, enroll=enroll)


def main() -> int:
    sources = load_sources()
    volume.check()
    workspace = Path(volume.configuration()['mount_point'])
    lock_root = volume.ENROLLMENT.parent
    status_path = workspace / 'migration' / 'source-staging-queue-v1.json'
    def execute(item):
        try:
            return run_source(item, workspace, lock_root)
        except (OSError, RuntimeError, ValueError, subprocess.TimeoutExpired) as error:
            # Preserve raw details in the private service log; public status
            # contains only a bounded class/reason and never advances a proof.
            state, reason = classify_error(error)
            if state == 'blocked':
                print(f"{item['name']}: {type(error).__name__}: {error}", file=sys.stderr, flush=True)
            return {'name': item['name'], 'state': state, 'reason': reason,
                    'error_type': type(error).__name__}
    # Persist the queue before launching work and after each completed lane.
    # A multi-hour source must not hide another source's wait or completion.
    # A queued observation is neither a worker heartbeat nor a copy proof.
    results = [{'name': item['name'], 'state': 'queued', 'reason': 'staging_queued'} for item in sources]

    enrollment = None
    bybit_enrollment = None
    options_raw_enrollment = None
    def publish():
        value = {'contract': CONTRACT, 'observed_at_epoch': time.time(), 'sources': results,
                 'source_files_deleted': 0, 'source_aliases_changed': False,
                 'D_backup_claimed': False, 'NAS_backup_claimed': False}
        if enrollment is not None:
            value['minute_raw_handoff_enrollment'] = enrollment
        if bybit_enrollment is not None:
            value['bybit_handoff_enrollment'] = bybit_enrollment
        if options_raw_enrollment is not None:
            value['options_raw_handoff_enrollment'] = options_raw_enrollment
        atomic_write_json(status_path, value, durable=True)
        return value

    publish()
    # Disjoint enrolled roots may overlap their I/O. The original per-source
    # locks and shared E reservation still decide each individual admission.
    if sources:
        with ThreadPoolExecutor(max_workers=min(2, len(sources)), thread_name_prefix='workspace-source') as pool:
            futures = {pool.submit(execute, item): index for index, item in enumerate(sources)}
            for future in as_completed(futures):
                results[futures[future]] = future.result()
                publish()
    enrollment = auto_enroll_minute_raw(results)
    bybit_enrollment = auto_enroll_bybit(results)
    options_raw_enrollment = auto_enroll_options_raw(results)
    value = publish()
    print(json.dumps(value, ensure_ascii=False), flush=True)
    return (0 if all(row['state'] in {'copy_verified', 'source_stream_staged'} for row in results)
            and all(v.get('state') != 'blocked' for v in (enrollment, bybit_enrollment,
                                                          options_raw_enrollment)) else 75)


if __name__ == '__main__':
    raise SystemExit(main())
