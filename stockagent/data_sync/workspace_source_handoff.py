"""Guard explicitly enrolled C-to-E handoffs without changing data owners.

The retained C root is not a retirement receipt. Only the named collectors
with reviewed root-lock, namespace and boot contracts can use this owner.
"""
from __future__ import annotations

from contextlib import ExitStack
from contextlib import contextmanager, nullcontext
from contextvars import ContextVar
import ctypes
from dataclasses import dataclass
import fcntl
import hashlib
import json
import math
import os
from pathlib import Path
import sqlite3
import stat
import subprocess
import sys
import time
import uuid

from downloader.artifact_io import atomic_write_json
from stockagent.data_sync import provider_workspace, workspace_relocation as relocation, workspace_volume as volume
from stockagent.data_sync.desync_snapshots import SnapshotError, _fsync_directory

ROOT = Path(__file__).resolve().parents[2]
COLD_PRIMARY = Path('/srv/stockagent-packed')
NAME = 'crypto-etf-history-e-primary-20261104-v1'
SOURCE = ROOT / 'data_crypto_etf'
BACKUP = ROOT / '.data_crypto_etf.pre-e-20261104-v1'
CONTROL = volume.ENROLLMENT.parent / 'source-handoff-v1'
JOURNAL = CONTROL / 'crypto-etf.json'
CONTRACT = 'stockagent.workspace_source_handoff.v1'
# Small copies retain the established in-memory verifier. Larger inventories
# must stream: otherwise a valid multi-million-file staging report cannot be
# handed off and an in-memory dictionary competes with live services.
IN_MEMORY_VERIFICATION_ENTRY_LIMIT = 100000
COIN_NAME = 'coinmetrics-community-e-primary-20261104-v1'
FOREX_NAME = 'forex-frankfurter-e-primary-20261104-v1'
BINANCE_NAME = 'binance-market-e-primary-20261104-v1'
BINANCE_ARCHIVE_NAME = 'binance-archive-e-primary-20261104-v1'
BYBIT_NAME = 'bybit-source-stream-e-20261108-v1'
OKX_NAME = 'okx-e-primary-20261104-v1'
LEGACY_PARQUET_NAME = 'legacy-parquet-e-primary-20261104-v1'
FREE_PUBLIC_NAME = 'free-public-context-e-primary-20261104-v1'
PUBLIC_ECONOMIC_NAME = 'public-economic-e-primary-20261104-v1'
NEWS_NAME = 'news-training-local-e-primary-20261104-v1'
NEWS_BODIES_NAME = 'news-fulltext-bodies-e-primary-20261104-v1'
NEWS_BODIES_MARKER = '20261027/20261027T171812670228Z-349dc38d.parquet'
HFT_NAME = 'tw-microstructure-train-e-primary-20261104-v1'
MINUTE_NAME = 'tw-minute-train-stream-e-20261108-v1'
MINUTE_RAW_NAME = 'tw-minute-source-stream-e-20261108-v1'
OPTIONS_RAW_NAME = 'tw-index-options-raw-stream-e-20261108-v1'
MINUTE_STREAM_SOURCES = frozenset({MINUTE_NAME, MINUTE_RAW_NAME})
SOURCE_STREAM_HANDOFF_SOURCES = MINUTE_STREAM_SOURCES | frozenset({BYBIT_NAME, OPTIONS_RAW_NAME})
FUTURES_NAME = 'tw-futures-prepared-e-primary-20261104-v1'
FUTURES_MARKER = 'tx_contract_days_audit_projection_20261030_v1/manifest.json'
FUTURES_725_NAME = 'tw-futures-universe725-e-projection-20261108-v1'
FUTURES_765_NAME = 'tw-futures-universe765-e-projection-20261108-v1'
YAHOO_NAME = 'yahoo-market-e-primary-20261104-v1'
PEPPERSTONE_NAME = 'forex-pepperstone-e-primary-20261104-v1'
CFTC_NAME = 'cftc-legacy-pre2000-e-primary-20261104-v1'
FRED_NAME = 'fred-crypto-macro-e-primary-20261104-v1'
TAIFEX_PUBLIC_NAME = 'taifex-public-history-e-primary-20261104-v1'
RETAINED_LOCK_SOURCES = {
    CFTC_NAME: {'directory':'data_cftc_legacy', 'label':'cftc-legacy-pre2000', 'marker':'manifest.json',
                'guard_files':('scripts/download_cftc_legacy_pre2000.py', 'scripts/run_cftc_legacy_pre2000.sh',
                               'scripts/publish_data_releases.py', 'stockagent/data_sync/closed_source.py')},
    FRED_NAME: {'directory':'data_fred_crypto_macro', 'label':'fred-crypto-macro', 'marker':'download_summary.json',
                'guard_files':('downloader/download_fred_crypto_macro_vintages.py',
                               'stockagent/data_sync/retained_source.py', 'scripts/publish_data_releases.py')},
    TAIFEX_PUBLIC_NAME: {'directory':'data_taifex_public_history', 'label':'taifex-public-history',
        'marker':'manifest.json', 'native_locks':('state/collector.lock', 'rules/state/collector.lock'),
        'guard_files':('stockagent/data_sync/taifex_workspace_guard.py',
                       'stockagent/data_sync/yahoo_workspace_guard.py',
                       'scripts/download_taifex_public_history.py', 'scripts/download_taifex_openapi_catalog.py',
                       'scripts/download_taifex_vix_recent.py', 'scripts/download_taifex_rule_history.py',
                       'scripts/run_taifex_public_history.sh', 'scripts/publish_data_releases.py')},
    OPTIONS_RAW_NAME: {'directory':'data_tw_index_options_daily/raw', 'label':'tw-index-options-raw-workspace',
        'marker':'annual/2001_opt.zip', 'native_locks':(),
        'external_native_locks':('data_tw_index_options_daily/state/registered_auxiliary_daily.lock',),
        'guard_files':('stockagent/data_sync/tw_option_raw_workspace_guard.py',
                       'stockagent/data_sync/yahoo_workspace_guard.py',
                       'scripts/download_taifex_option_daily_history.py',
                       'scripts/download_taifex_final_settlement_history.py',
                       'scripts/run_taifex_auxiliary_daily.sh',
                       'scripts/run_shioaji_taifex_bidask_stream.sh', 'scripts/publish_data_releases.py')},
}
FUTURES_SCOPED_SOURCES = frozenset({FUTURES_NAME, FUTURES_725_NAME, FUTURES_765_NAME})
FUTURES_SYMLINK_SOURCES = frozenset({FUTURES_NAME, FUTURES_765_NAME})
SCOPED_COPY_SOURCES = FUTURES_SCOPED_SOURCES | frozenset({TAIFEX_PUBLIC_NAME})
NEWS_SIDECAR_PATHS = frozenset({
    'versions/20261016T012637Z-fa6c4df0/build.sqlite3-shm',
    'versions/20261016T012637Z-fa6c4df0/build.sqlite3-wal',
})
LATE_ROOT_LOCK_SOURCES = frozenset({BINANCE_ARCHIVE_NAME, OKX_NAME, LEGACY_PARQUET_NAME,
                                    BYBIT_NAME,
                                    FREE_PUBLIC_NAME, PUBLIC_ECONOMIC_NAME, NEWS_NAME, NEWS_BODIES_NAME, HFT_NAME,
                                    *MINUTE_STREAM_SOURCES, FUTURES_NAME, FUTURES_725_NAME, FUTURES_765_NAME,
                                    YAHOO_NAME, PEPPERSTONE_NAME, *RETAINED_LOCK_SOURCES})
PRIOR_COPY_GENERATION_SOURCES = frozenset({LEGACY_PARQUET_NAME, FREE_PUBLIC_NAME, PUBLIC_ECONOMIC_NAME,
                                          BYBIT_NAME,
                                          NEWS_NAME, NEWS_BODIES_NAME, HFT_NAME, *MINUTE_STREAM_SOURCES, FUTURES_NAME,
                                          FUTURES_725_NAME, FUTURES_765_NAME, YAHOO_NAME, PEPPERSTONE_NAME,
                                          *RETAINED_LOCK_SOURCES})
HEAVY_SOURCES = frozenset({BINANCE_NAME, BINANCE_ARCHIVE_NAME, OKX_NAME, NEWS_NAME, NEWS_BODIES_NAME,
                           BYBIT_NAME,
                           HFT_NAME, *MINUTE_STREAM_SOURCES, FUTURES_NAME, FUTURES_725_NAME, FUTURES_765_NAME, YAHOO_NAME,
                           TAIFEX_PUBLIC_NAME, OPTIONS_RAW_NAME})
YAHOO_WRITER_GUARD_FILES = (
    'stockagent/data_sync/yahoo_workspace_guard.py',
    'downloader/download_yahoo_ohlcv.py', 'downloader/download_alpaca_us_ohlcv.py',
    'downloader/backfill_tw_public_to_yahoo.py', 'downloader/download_forex_frankfurter.py',
    'downloader/materialize_ohlcv_daily.py', 'downloader/quality_priority.py',
    'scripts/queue_source_anomaly_priorities.py', 'scripts/repair_fold_gt10_price_errors.py',
    'scripts/repair_fold_gt10_finmind_source.py', 'scripts/repair_tw_return_anomalies.py',
)
PEPPERSTONE_WRITER_GUARD_FILES = (*YAHOO_WRITER_GUARD_FILES, 'downloader/download_pepperstone.py',
                                 'scripts/publish_data_releases.py')
MINUTE_RAW_WRITER_GUARD_FILES = (
    'stockagent/data_sync/shioaji_minute_workspace_guard.py',
    'stockagent/data_sync/yahoo_workspace_guard.py',
    'downloader/download_shioaji_tw_minute_kbars.py',
    'scripts/rebuild_shioaji_minute_catalog.py',
    'scripts/run_shioaji_minute_full_backfill.sh',
    'scripts/shioaji_minute_locking.sh',
    'scripts/publish_data_releases.py',
)
BYBIT_WRITER_GUARD_FILES = (
    'stockagent/data_sync/bybit_workspace_guard.py', 'stockagent/data_sync/yahoo_workspace_guard.py',
    'downloader/download_bybit_perp_1m.py', 'downloader/download_bybit_perp_15m.py',
    'downloader/download_bybit_perp_daily.py', 'downloader/download_bybit_funding_history.py',
    'downloader/materialize_bybit_perpetual_daily.py', 'downloader/materialize_ohlcv_daily.py',
    'downloader/repair_bybit_1m_gaps.py', 'downloader/quality_priority.py',
    'scripts/build_bybit_crypto_public_daily_features.py', 'scripts/build_bybit_venue_daily_features.py',
    'scripts/queue_source_anomaly_priorities.py', 'scripts/write_data_readmes.py',
    'downloader/run_daily_all_markets.sh', 'scripts/publish_data_releases.py',
)
EXTRA_SOURCES = {
    LEGACY_PARQUET_NAME: ('data_parquet', 'legacy-parquet'),
    FREE_PUBLIC_NAME: ('data_free_public', 'free-public-context'),
    PUBLIC_ECONOMIC_NAME: ('data_public_economic', 'public-economic-history'),
    'crypto-public-context-e-primary-20261104-v1': ('data_crypto_historical_public', 'crypto-public-context'),
    'dune-crypto-e-primary-20261104-v1': ('data_dune_crypto', 'dune-crypto'),
    'keyed-public-catalogs-e-primary-20261104-v1': ('data_keyed_public_catalogs', 'keyed-public-catalogs'),
    'crypto-reference-e-primary-20261104-v1': ('data_crypto_reference', 'crypto-reference'),
    FOREX_NAME: ('data_forex_frankfurter', 'forex-frankfurter'),
    BINANCE_NAME: ('data_binance', 'binance-market'),
    BINANCE_ARCHIVE_NAME: ('data_binance_archive', 'binance-public-archive'),
}


@dataclass(frozen=True)
class SourceScope:
    name: str
    source: Path
    backup: Path
    journal: Path


_SELECTED_SCOPE: ContextVar[SourceScope | None] = ContextVar('source_handoff_scope', default=None)


def registered_names():
    names = [NAME, COIN_NAME, *EXTRA_SOURCES]
    marker = CONTROL / 'okx-enrollment.json'
    journal = CONTROL / 'okx.json'
    # A previously activated alias must remain guarded and recoverable even
    # if its enrollment marker was accidentally removed after activation. An
    # invalid marker is still visible to this one source; it must not prevent
    # unrelated already-enrolled aliases from starting or recovering.
    if marker.exists() or marker.is_symlink() or journal.exists() or journal.is_symlink():
        names.append(OKX_NAME)
    marker, journal = CONTROL/'news-enrollment.json', CONTROL/'news-training-workspace.json'
    if marker.exists() or marker.is_symlink() or journal.exists() or journal.is_symlink():
        names.append(NEWS_NAME)
    marker, journal = CONTROL/'news-bodies-enrollment.json', CONTROL/'news-fulltext-bodies-workspace.json'
    if marker.exists() or marker.is_symlink() or journal.exists() or journal.is_symlink():
        names.append(NEWS_BODIES_NAME)
    marker, journal = CONTROL/'hft-enrollment.json', CONTROL/'hft-workspace.json'
    if marker.exists() or marker.is_symlink() or journal.exists() or journal.is_symlink():
        names.append(HFT_NAME)
    marker, journal = CONTROL/'minute-enrollment.json', CONTROL/'minute-workspace.json'
    if marker.exists() or marker.is_symlink() or journal.exists() or journal.is_symlink():
        names.append(MINUTE_NAME)
    marker, journal = CONTROL/'minute-raw-enrollment.json', CONTROL/'minute-raw-workspace.json'
    if marker.exists() or marker.is_symlink() or journal.exists() or journal.is_symlink():
        names.append(MINUTE_RAW_NAME)
    marker, journal = CONTROL/'bybit-enrollment.json', CONTROL/'bybit-market.json'
    if marker.exists() or marker.is_symlink() or journal.exists() or journal.is_symlink():
        names.append(BYBIT_NAME)
    marker, journal = CONTROL/'futures-enrollment.json', CONTROL/'futures-workspace.json'
    if marker.exists() or marker.is_symlink() or journal.exists() or journal.is_symlink():
        names.append(FUTURES_NAME)
    marker, journal = CONTROL/'futures-725-enrollment.json', CONTROL/'futures-725-workspace.json'
    if marker.exists() or marker.is_symlink() or journal.exists() or journal.is_symlink():
        names.append(FUTURES_725_NAME)
    marker, journal = CONTROL/'futures-765-enrollment.json', CONTROL/'futures-765-workspace.json'
    if marker.exists() or marker.is_symlink() or journal.exists() or journal.is_symlink():
        names.append(FUTURES_765_NAME)
    marker, journal = CONTROL/'yahoo-enrollment.json', CONTROL/'yahoo-market.json'
    if marker.exists() or marker.is_symlink() or journal.exists() or journal.is_symlink():
        names.append(YAHOO_NAME)
    marker, journal = CONTROL/'pepperstone-enrollment.json', CONTROL/'forex-pepperstone.json'
    if marker.exists() or marker.is_symlink() or journal.exists() or journal.is_symlink():
        names.append(PEPPERSTONE_NAME)
    for name, spec in RETAINED_LOCK_SOURCES.items():
        marker, journal = CONTROL/(spec['label']+'-enrollment.json'), CONTROL/(spec['label']+'.json')
        if marker.exists() or marker.is_symlink() or journal.exists() or journal.is_symlink():
            names.append(name)
    return tuple(names)


def _validate_okx_enrollment():
    marker = CONTROL / 'okx-enrollment.json'
    if marker.is_symlink() or not marker.exists():
        raise SnapshotError('OKX source handoff enrollment is missing or redirected')
    info = marker.lstat()
    if (not stat.S_ISREG(info.st_mode) or info.st_uid != os.geteuid()
            or info.st_mode & 0o022 or info.st_size > 4096):
        raise SnapshotError('OKX source handoff enrollment is not operator-owned')
    body = {'contract': 'stockagent.okx_source_handoff_enrollment.v1',
            'name': OKX_NAME, 'source': 'data_okx', 'marker': '1m/download_summary.json',
            'preserve_c_original': True}
    identity = hashlib.sha256(json.dumps(body, sort_keys=True, separators=(',', ':')).encode()).hexdigest()
    if json.loads(marker.read_bytes()) != {**body, 'identity_sha256': identity}:
        raise SnapshotError('OKX source handoff enrollment identity differs')


def _validate_news_enrollment():
    marker = CONTROL/'news-enrollment.json'
    if marker.is_symlink() or not marker.exists():
        raise SnapshotError('news source handoff enrollment is missing or redirected')
    info = marker.lstat()
    if (not stat.S_ISREG(info.st_mode) or info.st_uid != os.geteuid()
            or info.st_mode & 0o022 or info.st_size > 4096):
        raise SnapshotError('news source handoff enrollment is not operator-owned')
    body = {'contract': 'stockagent.news_source_handoff_enrollment.v1',
            'name': NEWS_NAME, 'source': 'data_news', 'marker': 'manifest.json',
            'private_only': True, 'preserve_c_original_until_sealed': True}
    identity = hashlib.sha256(json.dumps(body, sort_keys=True, separators=(',', ':')).encode()).hexdigest()
    if json.loads(marker.read_bytes()) != {**body, 'identity_sha256': identity}:
        raise SnapshotError('news source handoff enrollment identity differs')


def _validate_news_bodies_enrollment():
    marker = CONTROL/'news-bodies-enrollment.json'
    if marker.is_symlink() or not marker.exists():
        raise SnapshotError('news bodies handoff enrollment is missing or redirected')
    info = marker.lstat()
    if (not stat.S_ISREG(info.st_mode) or info.st_uid != os.geteuid()
            or info.st_mode & 0o022 or info.st_size > 4096):
        raise SnapshotError('news bodies handoff enrollment is not operator-owned')
    body = {'contract': 'stockagent.news_bodies_source_handoff_enrollment.v1',
            'name': NEWS_BODIES_NAME, 'source': 'data_news_fulltext/bodies',
            'marker': NEWS_BODIES_MARKER, 'private_only': True,
            'preserve_c_original_until_sealed': True}
    identity = hashlib.sha256(json.dumps(body, sort_keys=True, separators=(',', ':')).encode()).hexdigest()
    if json.loads(marker.read_bytes()) != {**body, 'identity_sha256': identity}:
        raise SnapshotError('news bodies handoff enrollment identity differs')


def _validate_hft_enrollment():
    marker = CONTROL/'hft-enrollment.json'
    if marker.is_symlink() or not marker.exists():
        raise SnapshotError('HFT source handoff enrollment is missing or redirected')
    info = marker.lstat()
    if (not stat.S_ISREG(info.st_mode) or info.st_uid != os.geteuid()
            or info.st_mode & 0o022 or info.st_size > 4096):
        raise SnapshotError('HFT source handoff enrollment is not operator-owned')
    body = {'contract': 'stockagent.hft_source_handoff_enrollment.v1', 'name': HFT_NAME,
            'source': 'data_tw_microstructure/hft_dataset', 'marker': 'manifest.json',
            'preserve_c_original_until_sealed': True}
    identity = hashlib.sha256(json.dumps(body, sort_keys=True, separators=(',', ':')).encode()).hexdigest()
    if json.loads(marker.read_bytes()) != {**body, 'identity_sha256': identity}:
        raise SnapshotError('HFT source handoff enrollment identity differs')


def _validate_minute_enrollment():
    marker = CONTROL/'minute-enrollment.json'
    if marker.is_symlink() or not marker.exists():
        raise SnapshotError('minute source handoff enrollment is missing or redirected')
    info = marker.lstat()
    if (not stat.S_ISREG(info.st_mode) or info.st_uid != os.geteuid()
            or info.st_mode & 0o022 or info.st_size > 4096):
        raise SnapshotError('minute source handoff enrollment is not operator-owned')
    body = {'contract': 'stockagent.minute_source_handoff_enrollment.v1', 'name': MINUTE_NAME,
            'source': 'data_tw_minute/research_dataset', 'marker': 'manifest.json',
            'preserve_c_original_until_sealed': True, 'destination_independent_readback': False}
    identity = hashlib.sha256(json.dumps(body, sort_keys=True, separators=(',', ':')).encode()).hexdigest()
    if json.loads(marker.read_bytes()) != {**body, 'identity_sha256': identity}:
        raise SnapshotError('minute source handoff enrollment identity differs')


def minute_raw_enrollment_value():
    hashes = {path: relocation.digest(ROOT/path, no_follow=True) for path in MINUTE_RAW_WRITER_GUARD_FILES}
    body = {'contract': 'stockagent.minute_raw_source_handoff_enrollment.v1', 'name': MINUTE_RAW_NAME,
            'source': 'data_tw_minute/shioaji_1m', 'marker': 'download_summary.json',
            'runner_lock': 'artifacts/data_repair/shioaji_minute_full/runner.lock',
            'writer_guard_sha256': hashes, 'preserve_c_original_until_sealed': True,
            'destination_independent_readback': False}
    identity = hashlib.sha256(json.dumps(body, sort_keys=True, separators=(',', ':')).encode()).hexdigest()
    return {**body, 'identity_sha256': identity}


def _validate_minute_raw_enrollment():
    marker = CONTROL/'minute-raw-enrollment.json'
    if marker.is_symlink() or not marker.exists():
        raise SnapshotError('raw minute source enrollment is missing or redirected')
    info = marker.lstat()
    if (not stat.S_ISREG(info.st_mode) or info.st_uid != os.geteuid()
            or info.st_mode & 0o022 or info.st_size > 8192):
        raise SnapshotError('raw minute source enrollment is not operator-owned')
    if json.loads(marker.read_bytes()) != minute_raw_enrollment_value():
        raise SnapshotError('raw minute source enrollment or writer guard differs')


def bybit_enrollment_value():
    hashes = {path: relocation.digest(ROOT/path, no_follow=True) for path in BYBIT_WRITER_GUARD_FILES}
    body = {'contract':'stockagent.bybit_source_handoff_enrollment.v1', 'name':BYBIT_NAME,
            'source':'service_data/stockagent-live/data_bybit', 'catalog_alias':'data_bybit',
            'marker':'download_summary.json', 'writer_guard_sha256':hashes,
            'preserve_c_original_until_sealed':True, 'destination_independent_readback':False}
    identity = hashlib.sha256(json.dumps(body, sort_keys=True, separators=(',', ':')).encode()).hexdigest()
    return {**body, 'identity_sha256':identity}


def _validate_bybit_enrollment():
    marker = CONTROL/'bybit-enrollment.json'
    if marker.is_symlink() or not marker.exists():
        raise SnapshotError('Bybit source handoff enrollment is missing or redirected')
    info = marker.lstat()
    if (not stat.S_ISREG(info.st_mode) or info.st_uid != os.geteuid()
            or info.st_mode & 0o022 or info.st_size > 8192):
        raise SnapshotError('Bybit source handoff enrollment is not operator-owned')
    if json.loads(marker.read_bytes()) != bybit_enrollment_value():
        raise SnapshotError('Bybit source handoff enrollment or writer guard differs')


def _validate_futures_enrollment():
    marker = CONTROL/'futures-enrollment.json'
    if marker.is_symlink() or not marker.exists():
        raise SnapshotError('futures source handoff enrollment is missing or redirected')
    info = marker.lstat()
    if (not stat.S_ISREG(info.st_mode) or info.st_uid != os.geteuid()
            or info.st_mode & 0o022 or info.st_size > 4096):
        raise SnapshotError('futures source handoff enrollment is not operator-owned')
    body = {'contract': 'stockagent.futures_source_handoff_enrollment.v1', 'name': FUTURES_NAME,
            'source': 'data_tw_index_futures/training_prepared', 'marker': FUTURES_MARKER,
            'preserve_c_original_until_sealed': True, 'preserve_internal_symlinks': True}
    identity = hashlib.sha256(json.dumps(body, sort_keys=True, separators=(',', ':')).encode()).hexdigest()
    if json.loads(marker.read_bytes()) != {**body, 'identity_sha256': identity}:
        raise SnapshotError('futures source handoff enrollment identity differs')


def _validate_futures_725_enrollment():
    marker = CONTROL/'futures-725-enrollment.json'
    if marker.is_symlink() or not marker.exists():
        raise SnapshotError('futures 725 source handoff enrollment is missing or redirected')
    info = marker.lstat()
    if (not stat.S_ISREG(info.st_mode) or info.st_uid != os.geteuid()
            or info.st_mode & 0o022 or info.st_size > 4096):
        raise SnapshotError('futures 725 source handoff enrollment is not operator-owned')
    body = {'contract':'stockagent.futures_725_source_handoff_enrollment.v1', 'name':FUTURES_725_NAME,
            'source':'data_tw_index_futures/training_prepared/tw_futures_universe725_20261025_v2',
            'marker':'requested_725.csv', 'preserve_c_original_until_sealed':True,
            'exclude_factorized_sibling':True}
    identity = hashlib.sha256(json.dumps(body,sort_keys=True,separators=(',', ':')).encode()).hexdigest()
    if json.loads(marker.read_bytes()) != {**body, 'identity_sha256':identity}:
        raise SnapshotError('futures 725 source handoff enrollment identity differs')


def futures_765_enrollment_value():
    body = {'contract':'stockagent.futures_765_source_handoff_enrollment.v1', 'name':FUTURES_765_NAME,
            'source':'data_tw_index_futures/training_prepared/tw_futures_universe765_20261027_v1',
            'marker':'financial_all_twd_v8/requested_universe.csv', 'preserve_c_original_until_sealed':True,
            'preserve_internal_symlink_count':1, 'exclude_factorized_sibling':True}
    identity = hashlib.sha256(json.dumps(body,sort_keys=True,separators=(',', ':')).encode()).hexdigest()
    return {**body, 'identity_sha256':identity}


def _validate_futures_765_enrollment():
    marker = CONTROL/'futures-765-enrollment.json'
    if marker.is_symlink() or not marker.exists():
        raise SnapshotError('futures 765 source handoff enrollment is missing or redirected')
    info = marker.lstat()
    if (not stat.S_ISREG(info.st_mode) or info.st_uid != os.geteuid()
            or info.st_mode & 0o022 or info.st_size > 4096):
        raise SnapshotError('futures 765 source handoff enrollment is not operator-owned')
    if json.loads(marker.read_bytes()) != futures_765_enrollment_value():
        raise SnapshotError('futures 765 source handoff enrollment identity differs')


def yahoo_enrollment_value():
    # Bind exactly the reviewed entrypoints before the first exchange; later
    # code changes require a new enrollment, rather than a stale assumption.
    hashes = {name: relocation.digest(ROOT/name, no_follow=True) for name in YAHOO_WRITER_GUARD_FILES}
    body = {'contract':'stockagent.yahoo_source_handoff_enrollment.v1', 'name':YAHOO_NAME,
            'source':'data_yahoo', 'marker':'download_summary.json',
            'writer_guard_sha256':hashes, 'preserve_c_original_until_sealed':True}
    identity = hashlib.sha256(json.dumps(body,sort_keys=True,separators=(',', ':')).encode()).hexdigest()
    return {**body, 'identity_sha256':identity}


def _validate_yahoo_enrollment():
    marker = CONTROL/'yahoo-enrollment.json'
    if marker.is_symlink() or not marker.exists():
        raise SnapshotError('Yahoo source handoff enrollment is missing or redirected')
    info = marker.lstat()
    if (not stat.S_ISREG(info.st_mode) or info.st_uid != os.geteuid()
            or info.st_mode & 0o022 or info.st_size > 8192):
        raise SnapshotError('Yahoo source handoff enrollment is not operator-owned')
    if json.loads(marker.read_bytes()) != yahoo_enrollment_value():
        raise SnapshotError('Yahoo source handoff enrollment or writer guard differs')


def pepperstone_enrollment_value():
    hashes = {name: relocation.digest(ROOT/name, no_follow=True) for name in PEPPERSTONE_WRITER_GUARD_FILES}
    body = {'contract':'stockagent.pepperstone_source_handoff_enrollment.v1', 'name':PEPPERSTONE_NAME,
            'source':'data_peperstone', 'marker':'download_summary.json',
            'writer_guard_sha256':hashes, 'preserve_c_original_until_sealed':True}
    identity = hashlib.sha256(json.dumps(body,sort_keys=True,separators=(',', ':')).encode()).hexdigest()
    return {**body, 'identity_sha256':identity}


def _validate_pepperstone_enrollment():
    marker = CONTROL/'pepperstone-enrollment.json'
    if marker.is_symlink() or not marker.exists():
        raise SnapshotError('Pepperstone source handoff enrollment is missing or redirected')
    info = marker.lstat()
    if (not stat.S_ISREG(info.st_mode) or info.st_uid != os.geteuid()
            or info.st_mode & 0o022 or info.st_size > 8192):
        raise SnapshotError('Pepperstone source handoff enrollment is not operator-owned')
    if json.loads(marker.read_bytes()) != pepperstone_enrollment_value():
        raise SnapshotError('Pepperstone source handoff enrollment or writer guard differs')


def retained_enrollment_value(name):
    spec = RETAINED_LOCK_SOURCES.get(name)
    if spec is None:
        raise SnapshotError('retained collector enrollment is not reviewed')
    hashes = {path: relocation.digest(ROOT/path, no_follow=True) for path in spec['guard_files']}
    body = {'contract':'stockagent.retained_collector_source_handoff_enrollment.v1',
            'name':name, 'source':spec['directory'], 'marker':spec['marker'],
            'collector_lock':'state/collector.lock', 'writer_guard_sha256':hashes,
            'preserve_c_original_until_sealed':True}
    if 'native_locks' in spec:
        body['collector_locks'] = list(spec['native_locks'])
    if name == OPTIONS_RAW_NAME:
        body.pop('collector_lock')
        body['external_collector_locks'] = list(spec['external_native_locks'])
    identity = hashlib.sha256(json.dumps(body,sort_keys=True,separators=(',', ':')).encode()).hexdigest()
    return {**body, 'identity_sha256':identity}


def _validate_retained_enrollment(name):
    spec = RETAINED_LOCK_SOURCES.get(name)
    if spec is None:
        raise SnapshotError('retained collector enrollment is not reviewed')
    marker = CONTROL/(spec['label']+'-enrollment.json')
    if marker.is_symlink() or not marker.exists():
        raise SnapshotError('retained collector enrollment is missing or redirected')
    info = marker.lstat()
    if (not stat.S_ISREG(info.st_mode) or info.st_uid != os.geteuid()
            or info.st_mode & 0o022 or info.st_size > 8192):
        raise SnapshotError('retained collector enrollment is not operator-owned')
    if json.loads(marker.read_bytes()) != retained_enrollment_value(name):
        raise SnapshotError('retained collector enrollment or writer code differs')


def _registered_scope(name: str) -> SourceScope:
    if name == NAME:
        return SourceScope(NAME, SOURCE, BACKUP, JOURNAL)
    if name == COIN_NAME:
        return SourceScope(COIN_NAME, ROOT / 'data_coinmetrics_community',
            ROOT / '.data_coinmetrics_community.pre-e-20261105-v1', CONTROL / 'coinmetrics.json')
    if name == OKX_NAME:
        return SourceScope(name, ROOT / 'data_okx', ROOT / '.data_okx.pre-e-20261106-v1',
                           CONTROL / 'okx.json')
    if name == NEWS_NAME:
        return SourceScope(name, ROOT / 'data_news', ROOT / '.data_news.pre-e-20261108-v1',
                           CONTROL / 'news-training-workspace.json')
    if name == NEWS_BODIES_NAME:
        return SourceScope(name, ROOT / 'data_news_fulltext/bodies',
                           ROOT / '.data_news_fulltext_bodies.pre-e-20261108-v1',
                           CONTROL / 'news-fulltext-bodies-workspace.json')
    if name == HFT_NAME:
        return SourceScope(name, ROOT / 'data_tw_microstructure/hft_dataset',
                           ROOT / '.data_tw_microstructure_hft.pre-e-20261108-v1',
                           CONTROL / 'hft-workspace.json')
    if name == MINUTE_NAME:
        return SourceScope(name, ROOT / 'data_tw_minute/research_dataset',
                           ROOT / '.data_tw_minute_research.pre-e-20261108-v1',
                           CONTROL / 'minute-workspace.json')
    if name == MINUTE_RAW_NAME:
        return SourceScope(name, ROOT / 'data_tw_minute/shioaji_1m',
                           ROOT / '.data_tw_minute_shioaji_1m.pre-e-20261108-v1',
                           CONTROL / 'minute-raw-workspace.json')
    if name == BYBIT_NAME:
        return SourceScope(name, ROOT/'service_data/stockagent-live/data_bybit',
                           ROOT/'.data_bybit.pre-e-20261108-v1', CONTROL/'bybit-market.json')
    if name == FUTURES_NAME:
        return SourceScope(name, ROOT / 'data_tw_index_futures/training_prepared',
                           ROOT / '.data_tw_index_futures_training.pre-e-20261108-v1',
                           CONTROL / 'futures-workspace.json')
    if name == FUTURES_725_NAME:
        return SourceScope(name, ROOT/'data_tw_index_futures/training_prepared/tw_futures_universe725_20261025_v2',
                           ROOT/'.data_tw_futures_universe725.pre-e-20261108-v1',
                           CONTROL/'futures-725-workspace.json')
    if name == FUTURES_765_NAME:
        return SourceScope(name, ROOT/'data_tw_index_futures/training_prepared/tw_futures_universe765_20261027_v1',
                           ROOT/'.data_tw_futures_universe765.pre-e-20261108-v1',
                           CONTROL/'futures-765-workspace.json')
    if name == YAHOO_NAME:
        return SourceScope(name, ROOT/'data_yahoo', ROOT/'.data_yahoo.pre-e-20261108-v1',
                           CONTROL/'yahoo-market.json')
    if name == PEPPERSTONE_NAME:
        return SourceScope(name, ROOT/'data_peperstone', ROOT/'.data_peperstone.pre-e-20261108-v1',
                           CONTROL/'forex-pepperstone.json')
    if name == OPTIONS_RAW_NAME:
        return SourceScope(name, ROOT/'data_tw_index_options_daily/raw',
                           ROOT/'.data_tw_index_options_daily_raw.pre-e-20261108-v1',
                           CONTROL/'tw-index-options-raw-workspace.json')
    if name in RETAINED_LOCK_SOURCES:
        spec = RETAINED_LOCK_SOURCES[name]
        return SourceScope(name, ROOT/spec['directory'], ROOT/('.'+spec['directory']+'.pre-e-20261108-v1'),
                           CONTROL/(spec['label']+'.json'))
    if name in EXTRA_SOURCES:
        directory, label = EXTRA_SOURCES[name]
        return SourceScope(name, ROOT / directory, ROOT / ('.' + directory + '.pre-e-20261105-v1'),
                           CONTROL / (label + '.json'))
    raise ValueError('source handoff is not explicitly enrolled')


def _scope() -> SourceScope:
    return _SELECTED_SCOPE.get() or _registered_scope(NAME)


@contextmanager
def selected_source(name: str):
    """Pin the reviewed source identity for one operation or observation."""
    token = _SELECTED_SCOPE.set(_registered_scope(name))
    try:
        yield
    finally:
        _SELECTED_SCOPE.reset(token)


def inode(path: Path) -> list[int]:
    s = path.stat()
    return [s.st_dev, s.st_ino]


def enrolled():
    from scripts import run_workspace_source_staging as queue
    scope = _scope()
    if scope.name in {FUTURES_725_NAME, FUTURES_765_NAME}:
        from scripts import project_futures_725_workspace_index as projected
        projected_scope = projected.FUTURES_765 if scope.name == FUTURES_765_NAME else projected.default_scope()
        matches = [projected.handoff_item(projected_scope)]
    else:
        matches = [item for item in queue.load_sources() if item['name'] == scope.name]
    if len(matches) != 1 or matches[0]['source'] != scope.source.relative_to(ROOT).as_posix():
        raise SnapshotError('source handoff staging enrollment differs')
    workspace = Path(volume.configuration()['mount_point'])
    source, destination, evidence, report = queue.paths(matches[0], workspace)
    if source != scope.source:
        raise SnapshotError('source handoff root differs')
    return matches[0], workspace, destination, evidence, report


def read_journal():
    scope = _scope()
    if not scope.journal.exists():
        if scope.journal.is_symlink() or scope.backup.exists() or scope.backup.is_symlink():
            raise SnapshotError('source handoff journal is missing for a retained root')
        return None
    if scope.journal.is_symlink() or scope.journal.stat().st_size > 128 * 1024:
        raise SnapshotError('source handoff journal is redirected or too large')
    value = json.loads(scope.journal.read_bytes())
    _, workspace, destination, evidence, _ = enrolled()
    if (not isinstance(value, dict) or value.get('contract') != CONTRACT
            or value.get('source') != str(scope.source) or value.get('backup') != str(scope.backup)
            or value.get('destination') != str(destination) or value.get('evidence') != str(evidence)
            or value.get('state') not in {'preparing', 'prepared', 'exchanged', 'active'}
            or value.get('workspace') != str(workspace)):
        raise SnapshotError('source handoff journal identity differs')
    return value


def save(value):
    CONTROL.mkdir(mode=0o700, parents=True, exist_ok=True)
    if CONTROL.is_symlink() or CONTROL.resolve(strict=True) != CONTROL:
        raise SnapshotError('source handoff control root is redirected')
    atomic_write_json(_scope().journal, value, durable=True)


def guard_path(path: Path):
    """A registered collector must never write the C mount placeholder."""
    path = Path(os.path.abspath(path))
    for name in registered_names():
        with selected_source(name):
            source = _scope().source
            if path != source and not path.is_relative_to(source):
                continue
            value = read_journal()
            if value is None:
                return None
            if value['state'] != 'active':
                raise SnapshotError('source handoff is in progress; collector waits')
            volume.check()
            if not provider_workspace.exact_mount(source, Path(value['destination'])):
                raise SnapshotError('source E mount is missing; C fallback refused')
            return value
    return None


def guard_locked_path(path: Path, handle, *, lock_name='.download.lock'):
    """A lock opened before an alias change cannot authorize the new path."""
    path = Path(path)
    if lock_name not in {'.download.lock', '.quality_priority.lock', '.bea.lock', '.census.lock', '.moi.lock',
                         '.build.lock'}:
        raise SnapshotError('unrecognized source handoff lock')
    guard_path(path)
    try:
        current = (path / lock_name).lstat()
        held = os.fstat(handle.fileno())
    except OSError as error:
        raise SnapshotError('source lock disappeared during handoff') from error
    if (not stat.S_ISREG(current.st_mode) or (current.st_dev, current.st_ino) != (held.st_dev, held.st_ino)):
        raise SnapshotError('source lock inode changed during handoff')


def _safe_relative(name):
    p = Path(name)
    if (not isinstance(name, str) or not name or p.is_absolute() or '..' in p.parts
            or p.as_posix() != name):
        raise SnapshotError('source copy index contains an unsafe name')
    return p


def _canonical_c_source(source: Path, current_device: int) -> bool:
    try:
        return (not source.is_symlink() and source.resolve(strict=True) == source
                and source.is_relative_to(ROOT) and source.stat().st_dev == current_device
                and ROOT.stat().st_dev == current_device and Path('/').stat().st_dev == current_device)
    except OSError:
        return False


def _source_device_rebind(source: Path, recorded, current):
    """Recognize a WSL device-number change on the same enrolled C root."""
    if (not isinstance(recorded, list) or not isinstance(current, list)
            or len(recorded) != 9 or len(current) != 9 or recorded[0] == current[0]
            or any(type(value) is not int or value < 0 for value in recorded+current)
            or any(recorded[index] != current[index] for index in (1, 2, 6, 7, 8))):
        return None
    if not _canonical_c_source(source, current[0]):
        return None
    return recorded[0], current[0]


def _same_source_generation(actual, expected, rebind):
    if actual == expected:
        return True
    return (rebind is not None and isinstance(actual, list) and isinstance(expected, list)
            and len(actual) == len(expected) == 9 and expected[0] == rebind[0]
            and actual[0] == rebind[1] and actual[1:] == expected[1:])


def _verified_news_sidecar_override(name, actual, expected, digest, source, rebind):
    """Allow only unchanged bytes with clock-only drift in two fixed sidecars."""
    if (_scope().name != NEWS_NAME or name not in NEWS_SIDECAR_PATHS
            or not isinstance(actual, list) or not isinstance(expected, list)
            or len(actual) != 9 or len(expected) != 9 or not stat.S_ISREG(actual[2])
            or actual[3] > 32768 or not isinstance(digest, str) or len(digest) != 64):
        return None
    comparable = actual.copy()
    comparable[4], comparable[5] = expected[4], expected[5]
    if not _same_source_generation(comparable, expected, rebind):
        return None
    member = source/_safe_relative(name)
    if relocation.signature(member) != actual:
        return None
    if relocation.digest(member, no_follow=True) != digest or relocation.signature(member) != actual:
        return None
    normalized = actual.copy()
    normalized[0] = expected[0]
    return normalized


def _same_linked_lock(actual, expected, rebind):
    """Only the enrolled placeholder hardlink may alter this lock's ctime."""
    if not isinstance(actual, list) or not isinstance(expected, list) or len(actual) != 9 or len(expected) != 9:
        return False
    device_matches = (actual[0] == expected[0] or
                      rebind is not None and (expected[0], actual[0]) == rebind)
    return (device_matches and all(actual[i] == expected[i] for i in (1, 2, 3, 4, 6, 7))
            and actual[8] == expected[8] + 1)


def _fixed_copy_completion_ns(evidence: Path, report: dict) -> int:
    """Bind a metadata-only recheck to the fixed, scope-specific copy receipt."""
    from stockagent.data_sync.packed_member_stream import read_stable_bytes
    raw = read_stable_bytes(evidence/'copy-result.json', maximum=128 * 1024)
    if hashlib.sha256(raw).hexdigest() != report.get('relocation_evidence_sha256'):
        raise SnapshotError('fixed copy receipt changed before source handoff')
    copy = json.loads(raw)
    completed = copy.get('observed_at_epoch') if isinstance(copy, dict) else None
    full_readback = (isinstance(copy, dict) and
                     (copy.get('contract') == 'workspace_exact_relocation_v1' or
                      _scope().name in SCOPED_COPY_SOURCES and copy.get('contract') == 'workspace_scoped_relocation_v1'
                      and copy.get('selected_source_sha256_and_destination_readback_verified') is True
                      and copy.get('excluded_subtrees') == [])
                     and copy.get('state') == 'exact_destination_readback_verified'
                     and copy.get('all_source_sha256_and_destination_readback_verified') is True)
    source_stream = (isinstance(copy, dict) and _scope().name in SOURCE_STREAM_HANDOFF_SOURCES
                     and copy.get('contract') == 'stockagent.source_stream_staging.v1'
                     and copy.get('state') == 'source_sha256_streamed_no_destination_readback'
                     and copy.get('source_sha256_streamed') is True
                     and copy.get('destination_independent_readback') is False
                     and copy.get('all_source_sha256_and_destination_readback_verified') is False)
    if (not (full_readback or source_stream)
            or copy.get('source_namespace_unchanged') is not True
            or copy.get('source_files_deleted') != 0 or copy.get('files') != report.get('files')
            or copy.get('logical_bytes') != report.get('logical_bytes')
            or type(completed) not in (int, float) or not math.isfinite(completed)
            or completed <= 0 or completed > time.time() + 120):
        raise SnapshotError('fixed copy receipt cannot support a generation-only recheck')
    return int(completed * 1_000_000_000) + 2048


def _supplemental_lock_signature(root: Path):
    """Bind a late-added, empty writer lock without treating it as data."""
    path = root / '.download.lock'
    before = relocation.signature(path)
    if (not stat.S_ISREG(before[2]) or before[3] != 0
            or before[6] != os.geteuid() or before[8] not in {1, 2}):
        raise SnapshotError('supplemental source lock is not an empty owned regular file')
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC)
    try:
        held = os.fstat(fd)
        if ((held.st_dev, held.st_ino) != tuple(before[:2]) or held.st_size != 0
                or os.read(fd, 1) or relocation.signature(path) != before):
            raise SnapshotError('supplemental source lock changed during observation')
    finally:
        os.close(fd)
    return before


def binance_handoff_window_allowed(*, for_start: bool) -> bool:
    """New scans require a long runway; an in-flight scan yields before open."""
    from datetime import datetime, time as wall_time
    from zoneinfo import ZoneInfo
    from scripts.check_outside_tw_opening_resource_window import evaluate
    return bool(evaluate(datetime.now(ZoneInfo('Asia/Taipei')),
        minimum_runway_minutes=500 if for_start else 0,
        protected_until=wall_time(13, 30), official_calendar_root=ROOT / 'data_tw_public')['allowed'])


def options_raw_handoff_window_allowed(*, for_start: bool, observed=None) -> bool:
    """Leave the actual TAIFEX capture runner and its settlement bootstrap free."""
    from datetime import datetime
    from stockagent.data.taifex_sessions import TAIPEI, next_taifex_capture_window
    local = (observed or datetime.now(TAIPEI)).astimezone(TAIPEI)
    window = next_taifex_capture_window(local)
    # This is the same normal-weekly clock used by the running capture owner.
    # Treat the whole capture window as protected and allow a wider lead for
    # starting a handoff than for yielding an already-running metadata scan.
    remaining = (window.starts_at-local).total_seconds()
    return remaining >= (300 if for_start else 60)


def _source_work_window_guard():
    """A bounded check between members, never a forceful writer interruption."""
    next_check = 0.0
    def guard():
        nonlocal next_check
        if _scope().name not in HEAVY_SOURCES:
            return
        now = time.monotonic()
        if now >= next_check:
            if (not binance_handoff_window_allowed(for_start=False)
                    or _scope().name == OPTIONS_RAW_NAME
                    and not options_raw_handoff_window_allowed(for_start=False)):
                raise BlockingIOError('tw_day_trade_priority_window')
            next_check = now + 30.0
    return guard


def _verify_large_current_copy(connection, *, count, source, destination, index,
                               index_before, wal, report, report_path,
                               lock_signature=None, progress_pass=None, work_guard=None,
                               supplemental_locks=None, source_rebind=None,
                               target_unchanged_before_ns=None):
    """Verify an enrolled large copy without materializing its entire index.

    The original index's unique path key lets the two namespace walks compare
    membership and count without an in-memory set. A prior, fixed copy can
    instead be checked by its unchanged file generations; this is not an
    acceptance or a handoff by itself.
    """
    columns = connection.execute('PRAGMA table_info(files)').fetchall()
    primary = [row for row in columns if row[5]]
    if len(primary) != 1 or primary[0][1] != 'path' or primary[0][5] != 1:
        raise SnapshotError('source copy index must have its unique path key')

    def decode(row):
        name, kind, sig, digest, size, state = row
        _safe_relative(name)
        allowed = {'directory', 'file', 'symlink'} if _scope().name in FUTURES_SYMLINK_SOURCES else {'directory', 'file'}
        if kind not in allowed:
            raise SnapshotError('source copy index contains duplicate or unsupported names')
        signature = json.loads(sig)
        if name == '.download.lock' and lock_signature is not None:
            if not _same_linked_lock(lock_signature, signature, source_rebind):
                raise SnapshotError('source lock generation differs beyond the enrolled hardlink')
            signature = lock_signature
        if kind == 'file' and (state != 'verified' or not isinstance(digest, str)
                               or len(digest) != 64 or size != signature[3]):
            raise SnapshotError('source copy index lacks a verified file')
        if kind == 'symlink' and (state != 'verified' or not isinstance(digest, str)
                                  or len(digest) != 64 or size != 0):
            raise SnapshotError('source copy index lacks a verified symlink')
        return name, (kind, signature, digest, size)

    files = logical_bytes = 0
    for raw in connection.execute('SELECT path,kind,sig,sha,bytes,state FROM files'):
        _, (kind, _, _, size) = decode(raw)
        if kind == 'file':
            files += 1
            logical_bytes += size
    if files != report['files']:
        raise SnapshotError('source copy file count differs')
    if logical_bytes != report['logical_bytes']:
        raise SnapshotError('source copy byte count differs')

    progress = (_RecheckProgress(progress_pass, files, logical_bytes)
                if progress_pass else None)

    # Preserve an order-independent, bounded-memory fingerprint of the exact
    # E generations whose bytes were hashed. A final directory walk must see
    # the same inode/size/mtime/ctime, not merely the same portable metadata.
    # The full SHA for every payload remains the authoritative byte proof.
    modulus = 1 << 256
    def generation_token(name, signature):
        payload = json.dumps([name, signature], ensure_ascii=True, separators=(',', ':')).encode('ascii')
        return int.from_bytes(hashlib.sha256(payload).digest(), 'big')

    def add_generation(accumulator, token):
        accumulator[0] ^= token
        accumulator[1] = (accumulator[1] + token) % modulus
        accumulator[2] += 1

    source_overrides = {}
    namespace_pass = 0

    def check_namespace(root, *, original):
        nonlocal namespace_pass
        namespace_pass += 1
        if progress:
            progress.begin_namespace(count, namespace_pass)
        seen = 0
        generations = [0, 0, 0]
        batch_size = min(128, connection.getlimit(sqlite3.SQLITE_LIMIT_VARIABLE_NUMBER))
        entries = relocation.walk(root, preserve_symlinks=_scope().name in FUTURES_SYMLINK_SOURCES)
        for batch in relocation._namespace_batches(entries, batch_size):
            names = [name for name, _, _ in batch
                     if supplemental_locks is None or name != '.download.lock']
            rows = {}
            if names:
                placeholders = ','.join('?' for _ in names)
                rows = {row[0]: row for row in connection.execute(
                    f'SELECT path,kind,sig,sha,bytes,state FROM files WHERE path COLLATE BINARY IN ({placeholders})',
                    names)}
            for name, kind, signature in batch:
                if work_guard:
                    work_guard()
                if supplemental_locks is not None and name == '.download.lock':
                    expected_lock = supplemental_locks['source' if original else 'destination']
                    if kind != 'file' or signature != expected_lock:
                        raise SnapshotError('supplemental source lock generation changed')
                    continue
                raw = rows.get(name)
                if raw is None:
                    raise SnapshotError('source or E exact namespace differs')
                indexed_name, (indexed_kind, expected, digest, _) = decode(raw)
                if indexed_name != name or indexed_kind != kind:
                    raise SnapshotError('source or E exact namespace differs')
                if original and not _same_source_generation(signature, expected, source_rebind):
                    override = _verified_news_sidecar_override(name, signature, expected, digest, source, source_rebind)
                    if override is None or name in source_overrides and source_overrides[name] != override:
                        raise SnapshotError('source generation changed since its fixed copy')
                    source_overrides[name] = override
                elif original and name in source_overrides:
                    raise SnapshotError('source sidecar generation changed during handoff')
                if not original and kind in {'directory', 'symlink'} and (
                        stat.S_IMODE(signature[2]) != stat.S_IMODE(expected[2])
                        or signature[4] != expected[4] or signature[6:8] != expected[6:8]):
                    raise SnapshotError('E source directory or symlink metadata differs')
                if not original and kind == 'symlink' and target_unchanged_before_ns is not None and signature[5] > target_unchanged_before_ns:
                    raise SnapshotError('E source symlink changed after its fixed copy receipt')
                if not original and kind == 'file':
                    if not relocation.signature_metadata_matches(signature, expected):
                        raise SnapshotError('E source file metadata differs')
                    if (_scope().name in FUTURES_SCOPED_SOURCES and target_unchanged_before_ns is not None
                            and signature[5] > target_unchanged_before_ns):
                        raise SnapshotError('E source generation changed after its fixed copy receipt')
                    add_generation(generations, generation_token(name, signature))
                seen += 1
                if progress and seen % 512 == 0:
                    progress.advance_namespace(seen)
        # A real directory cannot have two entries with the same name and
        # walk does not follow symlinks; unique index keys and equal counts prove
        # there is no indexed name missing from this namespace.
        if seen != count:
            raise SnapshotError('source or E namespace is incomplete')
        if progress:
            progress.advance_namespace(seen)
        return generations

    def check_file(row):
        name, (_, signature, expected_sha, size) = row
        origin, target = source / name, destination / name
        before_source, before_target = relocation.signature(origin), relocation.signature(target)
        if (not _same_source_generation(before_source, source_overrides.get(name, signature), source_rebind)
                or not relocation.signature_metadata_matches(before_target, signature)):
            raise SnapshotError('source or E member metadata differs')
        if target_unchanged_before_ns is not None:
            if before_target[5] > target_unchanged_before_ns:
                raise SnapshotError('E source generation changed after its fixed copy receipt')
        else:
            source_sha = relocation.digest(origin, no_follow=True)
            target_sha = relocation.digest(target, no_follow=True)
            if source_sha != expected_sha or target_sha != expected_sha:
                raise SnapshotError('source or E full SHA readback differs')
        if relocation.signature(origin) != before_source or relocation.signature(target) != before_target:
            raise SnapshotError('source or E member generation changed during handoff')
        return size, generation_token(name, before_target)

    def file_batches(rows):
        # A future per tiny member adds considerable Python scheduling cost
        # on million-file inventories. Limit both batch length and bytes so
        # big files still run independently and progress remains responsive.
        batch, pending_bytes = [], 0
        for row in rows:
            size = row[1][3]
            if batch and (len(batch) >= 32 or pending_bytes + size > 2 * 1024**2):
                yield tuple(batch)
                batch, pending_bytes = [], 0
            if size > 2 * 1024**2:
                yield (row,)
            else:
                batch.append(row)
                pending_bytes += size
        if batch:
            yield tuple(batch)

    def check_batch(rows):
        return tuple(check_file(row) for row in rows)

    link_counts = None
    try:
        if work_guard:
            work_guard()
        check_namespace(source, original=True)
        first_dest_generations = check_namespace(destination, original=False)
        if _scope().name in FUTURES_SCOPED_SOURCES and target_unchanged_before_ns is not None:
            # The two E namespace passes already bind every file's complete
            # generation and enforce the fixed-copy ctime fence. A separate
            # per-file stat pass would repeat millions of calls without adding
            # a payload check or a stronger generation identity.
            verified_generations = first_dest_generations
        else:
            if progress:
                if target_unchanged_before_ns is not None:
                    progress.begin_generation_check()
                else:
                    progress.begin_hashing()
            verified_generations = [0, 0, 0]
            rows = (decode(row) for row in connection.execute(
                "SELECT path,kind,sig,sha,bytes,state FROM files WHERE kind = 'file'"))
            for completed in relocation._bounded_as_ready(file_batches(rows), check_batch, workers=4):
                if work_guard:
                    work_guard()
                for size, token in completed:
                    add_generation(verified_generations, token)
                    if progress:
                        progress.advance(size)
        check_namespace(source, original=True)
        if check_namespace(destination, original=False) != verified_generations:
            raise SnapshotError('E file generation changed during the source handoff check'
                                if target_unchanged_before_ns is not None else
                                'E file generation changed after its full SHA readback')
        if _scope().name in FUTURES_SYMLINK_SOURCES:
            from stockagent.data_sync import workspace_symlink_closure
            link_counts = workspace_symlink_closure.inspect_links(
                connection, source, destination, source_device_mapping=source_rebind)
            if (link_counts['links'] != report.get('symlink_names_preserved')
                    or link_counts['unresolved_targets'] != 0
                    or link_counts['internal_targets_verified'] != link_counts['links']):
                raise SnapshotError('futures fixed copy has unresolved or uncounted symlinks')
        if supplemental_locks is not None and (
                relocation.signature(source) != supplemental_locks['source_root']
                or relocation.signature(destination) != supplemental_locks['destination_root']
                or _supplemental_lock_signature(source) != supplemental_locks['source']
                or _supplemental_lock_signature(destination) != supplemental_locks['destination']):
            raise SnapshotError('supplemental lock or source root changed during verification')
        if relocation.signature(index) != index_before or wal.exists() and wal.stat().st_size:
            raise SnapshotError('source copy index changed during handoff verification')
    except BlockingIOError:
        if progress:
            progress.finish('waiting')
        raise
    except BaseException:
        if progress:
            progress.finish('blocked')
        raise
    if progress:
        progress.finish('checked')
    return {'files': files, 'logical_bytes': logical_bytes,
            'copy_result_sha256': report['relocation_evidence_sha256'],
            'report_sha256': relocation.digest(report_path, no_follow=True),
            'marker_sha256': report['source_marker_sha256'],
            **({'internal_symlink_closure': link_counts} if link_counts is not None else {}),
            **({'source_generation_overrides': source_overrides} if source_overrides else {}),
            **({'handoff_verification_mode': ('source_stream_copy_generation_only' if _scope().name in SOURCE_STREAM_HANDOFF_SOURCES
                                               else 'prior_verified_copy_generation_only'),
                'new_payload_readback': False} if target_unchanged_before_ns is not None else {})}


class _RecheckProgress:
    """Small disposable observation; it never substitutes for the SHA proof."""
    def __init__(self, verification_pass, files, logical_bytes):
        scope = _scope()
        if scope.name not in registered_names()[1:] or verification_pass not in {'initial', 'preexchange'}:
            raise ValueError('unregistered source recheck observation')
        self.path = scope.journal.with_suffix('.progress.json')
        self.value = {'contract': 'stockagent.workspace_source_handoff_progress.v1',
                      'source': scope.name, 'state': 'running', 'phase': 'namespace',
                      'verification_pass': verification_pass, 'generation': uuid.uuid4().hex,
                      'completed_files': 0, 'total_files': files,
                      'completed_bytes': 0, 'total_bytes': logical_bytes}
        self.last_write = 0.0
        self.write(force=True)

    def write(self, *, force=False):
        now = time.monotonic()
        if not force and now - self.last_write < 1:
            return
        if self.path.is_symlink():
            raise SnapshotError('source recheck observation is redirected')
        self.value['observed_at_epoch'] = time.time()
        atomic_write_json(self.path, self.value)
        self.last_write = now

    def begin_hashing(self):
        self.value['phase'] = 'full_sha_recheck'
        self.write(force=True)

    def begin_namespace(self, entries, pass_number):
        self.value.update(phase='namespace', namespace_entries_checked=0,
                          namespace_entries_total=entries, namespace_pass=pass_number)
        self.write(force=True)

    def advance_namespace(self, entries):
        self.value['namespace_entries_checked'] = entries
        self.write()

    def advance_fence(self, directories, total):
        self.value.update(phase='fence_install', fence_directories_checked=directories,
                          fence_directories_total=total)
        self.write(force=directories in {0, total})

    def begin_generation_check(self):
        self.value['phase'] = 'generation_recheck'
        self.write(force=True)

    def advance(self, size):
        self.value['completed_files'] += 1
        self.value['completed_bytes'] += size
        self.write()

    def finish(self, state):
        self.value['state'] = state
        self.write(force=True)


def verify_current_copy(item, workspace, *, lock_signature=None, progress_pass=None, mutation_fence=None):
    """Recheck the whole fixed source and E copy under their actual locks."""
    from scripts import run_workspace_source_staging as queue
    work_guard = _source_work_window_guard()
    if mutation_fence is not None:
        original_guard = work_guard
        def work_guard():
            original_guard()
            mutation_fence.periodic()
    work_guard()
    source, destination, evidence, report_path = queue.paths(item, workspace)
    report = queue.verified_report(item, workspace)
    if report is None:
        raise SnapshotError('source handoff has no verified E copy')
    target_unchanged_before_ns = (_fixed_copy_completion_ns(evidence, report)
                                  if _scope().name in PRIOR_COPY_GENERATION_SOURCES else None)
    intent = json.loads((evidence / 'intent.json').read_bytes())
    recorded_root = intent.get('source_root_signature')
    current_root = relocation.signature(source)
    source_rebind = _source_device_rebind(source, recorded_root, current_root)
    archive_scope = _scope().name in LATE_ROOT_LOCK_SOURCES
    # The existing fixed copy predates this collector's root writer lock.
    # Only this enrolled source may add the empty lock: retain root inode,
    # mode, owner and link count; the complete old namespace and every byte
    # are still independently rechecked against the unchanged fixed index.
    if archive_scope:
        root_matches = (isinstance(recorded_root, list) and len(recorded_root) == 9
                        and (current_root[0] == recorded_root[0] or source_rebind is not None)
                        and all(current_root[i] == recorded_root[i] for i in (1, 2, 6, 7, 8)))
    else:
        root_matches = _same_source_generation(current_root, recorded_root, source_rebind)
    scoped_futures = (_scope().name in SCOPED_COPY_SOURCES
                      and intent.get('contract') == 'workspace_scoped_relocation_v1'
                      and intent.get('preserve_symlinks') is True
                      and intent.get('excluded_subtrees') == [])
    if (not (intent.get('contract') == 'workspace_exact_relocation_v1' or scoped_futures)
            or intent.get('source') != str(source) or intent.get('destination') != str(destination)
            or not root_matches):
        raise SnapshotError('source root generation differs from the fixed E copy')
    index = evidence / 'files.sqlite'
    wal = evidence / 'files.sqlite-wal'
    if (index.is_symlink() or not index.is_file() or wal.exists() and wal.stat().st_size):
        raise SnapshotError('source copy index is redirected or has an uncheckpointed WAL')
    index_before = relocation.signature(index)
    connection = sqlite3.connect(f'file:{index}?mode=ro', uri=True)
    try:
        count = connection.execute('SELECT count(*) FROM files').fetchone()[0]
        if count != intent.get('entries'):
            raise SnapshotError('source copy index has an unexpected scope')
        indexed_lock = bool(connection.execute(
            "SELECT 1 FROM files WHERE path = '.download.lock' COLLATE BINARY").fetchone())
        supplemental = archive_scope and not indexed_lock
        supplemental_locks = None
        if archive_scope and indexed_lock and not _same_source_generation(current_root, recorded_root, source_rebind):
            raise SnapshotError('source root generation differs from the fixed E copy')
        if supplemental:
            source_lock = _supplemental_lock_signature(source)
            destination_lock = _supplemental_lock_signature(destination)
            if lock_signature is not None and source_lock != lock_signature:
                raise SnapshotError('supplemental source lock differs from the handoff journal')
            supplemental_locks = {'source': source_lock, 'destination': destination_lock,
                                  'source_root': current_root,
                                  'destination_root': relocation.signature(destination)}
        if supplemental or count > IN_MEMORY_VERIFICATION_ENTRY_LIMIT or target_unchanged_before_ns is not None:
            return _verify_large_current_copy(connection, count=count, source=source,
                destination=destination, index=index, index_before=index_before, wal=wal,
                report=report, report_path=report_path, lock_signature=lock_signature,
                progress_pass=progress_pass, work_guard=work_guard,
                supplemental_locks=supplemental_locks, source_rebind=source_rebind,
                target_unchanged_before_ns=target_unchanged_before_ns)
        rows = list(connection.execute('SELECT path,kind,sig,sha,bytes,state FROM files'))
    finally:
        connection.close()
    if len(rows) != intent.get('entries'):
        raise SnapshotError('source copy index has an unexpected scope')
    expected = {}
    for name, kind, sig, digest, size, state in rows:
        _safe_relative(name)
        if name in expected or kind not in {'directory', 'file'}:
            raise SnapshotError('source copy index contains duplicate or unsupported names')
        signature = json.loads(sig)
        if name == '.download.lock' and lock_signature is not None:
            if not _same_linked_lock(lock_signature, signature, source_rebind):
                raise SnapshotError('source lock generation differs beyond the enrolled hardlink')
            signature = lock_signature
        if kind == 'file' and (state != 'verified' or not isinstance(digest, str)
                               or len(digest) != 64 or size != signature[3]):
            raise SnapshotError('source copy index lacks a verified file')
        expected[name] = (kind, signature, digest, size)
    if sum(row[0] == 'file' for row in expected.values()) != report['files']:
        raise SnapshotError('source copy file count differs')
    if sum(row[3] for row in expected.values() if row[0] == 'file') != report['logical_bytes']:
        raise SnapshotError('source copy byte count differs')

    progress = (_RecheckProgress(progress_pass, report['files'], report['logical_bytes'])
                if progress_pass else None)

    def check_namespace(root, *, original):
        seen = set()
        for name, kind, signature in relocation.walk(root):
            work_guard()
            row = expected.get(name)
            if row is None or row[0] != kind or name in seen:
                raise SnapshotError('source or E exact namespace differs')
            if original and not _same_source_generation(signature, row[1], source_rebind):
                raise SnapshotError('source generation changed since its fixed copy')
            if not original and kind == 'directory':
                expected_directory = row[1]
                if (stat.S_IMODE(signature[2]) != stat.S_IMODE(expected_directory[2])
                        or signature[4] != expected_directory[4]
                        or signature[6:8] != expected_directory[6:8]):
                    raise SnapshotError('E source directory metadata differs')
            seen.add(name)
        if len(seen) != len(expected):
            raise SnapshotError('source or E namespace is incomplete')

    try:
        check_namespace(source, original=True)
        check_namespace(destination, original=False)
    except BlockingIOError:
        if progress:
            progress.finish('waiting')
        raise
    except BaseException:
        if progress:
            progress.finish('blocked')
        raise

    def check_file(row):
        name, (kind, signature, expected_sha, _) = row
        if kind != 'file':
            return None
        origin, target = source / name, destination / name
        before_source, before_target = relocation.signature(origin), relocation.signature(target)
        if (not _same_source_generation(before_source, signature, source_rebind)
                or not relocation.signature_metadata_matches(before_target, signature)):
            raise SnapshotError('source or E member metadata differs')
        source_sha = relocation.digest(origin, no_follow=True)
        target_sha = relocation.digest(target, no_follow=True)
        if (source_sha != expected_sha or target_sha != expected_sha
                or relocation.signature(origin) != before_source or relocation.signature(target) != before_target):
            raise SnapshotError('source or E full SHA readback differs')
        return row[1][3]

    try:
        if progress:
            progress.begin_hashing()
        # Reuse the relocation owner's bounded, completion-order queue.
        # Executor.map eagerly creates one future per entry on supported Python
        # versions, and a large early file can hide completed small-file work.
        for size in relocation._bounded_as_ready(expected.items(), check_file, workers=4):
            work_guard()
            if progress and size is not None:
                progress.advance(size)
        check_namespace(source, original=True)
        check_namespace(destination, original=False)
        if relocation.signature(index) != index_before:
            raise SnapshotError('source copy index changed during handoff verification')
    except BlockingIOError:
        if progress:
            progress.finish('waiting')
        raise
    except BaseException:
        if progress:
            progress.finish('blocked')
        raise
    if progress:
        progress.finish('checked')
    return {'files': report['files'], 'logical_bytes': report['logical_bytes'],
            'copy_result_sha256': report['relocation_evidence_sha256'],
            'report_sha256': relocation.digest(report_path, no_follow=True),
            'marker_sha256': report['source_marker_sha256']}


def exchange(first: Path, second: Path):
    """Atomically retain the C root and expose the prebuilt lock placeholder."""
    libc = ctypes.CDLL(None, use_errno=True)
    rename = libc.renameat2
    rename.argtypes = (ctypes.c_int, ctypes.c_char_p, ctypes.c_int, ctypes.c_char_p, ctypes.c_uint)
    rename.restype = ctypes.c_int
    if rename(-100, os.fsencode(first), -100, os.fsencode(second), 2) != 0:
        code = ctypes.get_errno()
        raise OSError(code, os.strerror(code))
    _fsync_directory(first.parent)
    if first.parent != second.parent:
        _fsync_directory(second.parent)


def immutable_placeholder(path: Path):
    subprocess.run(['chattr', '+i', str(path)], check=True, capture_output=True, timeout=10)
    flags = subprocess.run(['lsattr', '-d', str(path)], check=True, capture_output=True,
                           text=True, timeout=10).stdout.split(maxsplit=1)[0]
    if 'i' not in flags:
        raise SnapshotError('source mount placeholder is not immutable')


def lock_file(path: Path):
    if path.is_symlink() or not stat.S_ISREG(path.lstat().st_mode):
        raise SnapshotError('source handoff lock is not its original regular inode')
    handle = path.open('r+b')
    try:
        fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BaseException:
        handle.close()
        raise
    return handle


def _lock_supplemental_file(path: Path):
    """Create/open only the late-added empty lock, then pin its actual inode."""
    fd = os.open(path, os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW | os.O_CLOEXEC, 0o600)
    handle = os.fdopen(fd, 'r+b')
    try:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        current = _supplemental_lock_signature(path.parent)
        held = os.fstat(handle.fileno())
        if (held.st_dev, held.st_ino) != tuple(current[:2]):
            raise SnapshotError('supplemental source lock was replaced while acquiring it')
    except BaseException:
        handle.close()
        raise
    return handle


@contextmanager
def guarded_source_writer(root: Path):
    """Hold the source inode shared with publication and an eventual E handoff."""
    root = Path(root)
    guard_path(root)
    root.mkdir(parents=True, exist_ok=True)
    with _lock_supplemental_file(root / '.download.lock') as handle:
        # The path may have changed while the lock was being opened. Both the
        # mounted generation and the actual lock inode must still match.
        guard_locked_path(root, handle)
        yield


def _lock_writer_roots(stack: ExitStack, original: Path, destination: Path):
    """Fence every reviewed writer on both fixed root identities."""
    if _scope().name == MINUTE_RAW_NAME:
        # The scheduled owner protects shell-written summaries as well as its
        # child downloader. Acquire it first, without waiting, so a live
        # backfill never loses priority to a migration trying the root locks.
        stack.enter_context(lock_file(ROOT/'artifacts/data_repair/shioaji_minute_full/runner.lock'))
    if _scope().name == BYBIT_NAME:
        stack.enter_context(lock_file(ROOT/'artifacts/daily_downloader/bybit_source_publish.lock'))
    for root in (original, destination):
        lock = root / '.download.lock'
        stack.enter_context(_lock_supplemental_file(lock) if _scope().name in LATE_ROOT_LOCK_SOURCES
                            else lock_file(lock))
    if _scope().name == FOREX_NAME:
        # The provider-qualified v2 extension also has an independent lock
        # and may be invoked directly without the legacy root collector.
        for root in (original, destination):
            stack.enter_context(lock_file(root / 'official_v2/.download.lock'))
    if _scope().name == BINANCE_NAME:
        # The Binance collector and daily materializer use the grain locks,
        # not the old root lock alone. Fence all present canonical grains on
        # BOTH source generations in a fixed order before either verification
        # or exchange. An unexpected namespace change fails closed.
        for grain in ('1m', '15m', 'daily'):
            directories = [root / grain for root in (original, destination)]
            present = [directory.exists() for directory in directories]
            if present[0] != present[1]:
                raise SnapshotError('Binance source grain namespace differs')
            if not present[0]:
                continue
            for directory in directories:
                if directory.is_symlink() or not directory.is_dir():
                    raise SnapshotError('Binance source grain is redirected')
                stack.enter_context(lock_file(directory / '.download.lock'))
    if _scope().name == OKX_NAME:
        # The collector/materializer and the independent quality-intent owner
        # write under separate original locks; fence both generations.
        for relative in ('1m/.download.lock', 'daily/.download.lock', '1m/.quality_priority.lock'):
            for root in (original, destination):
                stack.enter_context(lock_file(root / relative))
    if _scope().name == BYBIT_NAME:
        # Existing grain/quality locks remain authoritative for old processes.
        # Never create a missing nested lock and mutate a staged directory.
        for relative in ('1m/.download.lock', 'daily/.download.lock', '1m/.quality_priority.lock'):
            paths = [root/relative for root in (original, destination)]
            exists = [path.exists() or path.is_symlink() for path in paths]
            if exists[0] != exists[1]:
                raise SnapshotError('Bybit source grain lock namespace differs')
            if all(exists):
                for path in paths:
                    stack.enter_context(lock_file(path))
    if _scope().name == PUBLIC_ECONOMIC_NAME:
        # Independent providers keep their own locks and run concurrently.
        # The migration holds all three on both roots before alias exchange.
        for relative in ('.bea.lock', '.census.lock', '.moi.lock'):
            for root in (original, destination):
                stack.enter_context(lock_file(root / relative))
    if _scope().name == NEWS_NAME:
        # The builder and private relay share this existing lock. Hold both
        # roots while switching, including readers holding the shared lock.
        for root in (original, destination):
            stack.enter_context(lock_file(root / '.build.lock'))
    if _scope().name == NEWS_BODIES_NAME:
        # The queue's actual owner lives outside the subtree being mounted.
        stack.enter_context(lock_file(ROOT / 'data_news_fulltext/owner.lock'))
    if _scope().name in RETAINED_LOCK_SOURCES:
        # Fence every reviewed native lock on both exact generations. Writers
        # also hold the root lock, preventing stale waiters from writing to C.
        spec = RETAINED_LOCK_SOURCES[_scope().name]
        for relative in spec.get('native_locks', ('state/collector.lock',)):
            for root in (original, destination):
                stack.enter_context(lock_file(root/relative))
        for relative in spec.get('external_native_locks', ()):
            stack.enter_context(lock_file(ROOT/relative))


def _assert_retained(value):
    """Allow a reboot's device renumbering only within the same guarded C/E roots."""
    _, workspace, destination, evidence, _ = enrolled()
    backup = _scope().backup
    volume.check()
    if (backup.is_symlink() or not backup.is_dir()
            or backup.stat().st_dev != ROOT.stat().st_dev
            or backup.stat().st_ino != value.get('original_inode')
            or destination.is_symlink() or not destination.is_dir()
            or destination.stat().st_dev != workspace.stat().st_dev
            or destination.stat().st_ino != value.get('destination_inode')
            or relocation.digest(evidence / 'copy-result.json', no_follow=True) != value.get('copy_result_sha256')):
        raise SnapshotError('retained C root, E inode, volume, or fixed copy proof differs')
    lock = backup / '.download.lock'
    if (lock.is_symlink() or not stat.S_ISREG(lock.lstat().st_mode)
            or lock.stat().st_dev != ROOT.stat().st_dev
            or lock.stat().st_ino != value.get('original_lock_inode')):
        raise SnapshotError('retained original collector lock differs')
    return destination


def mount_everywhere(value):
    """Attach the fixed E source in each pinned visible repository namespace."""
    from scripts import relocate_provider_workspace as providers
    from stockagent.data_sync.transport_namespace_handoff import prepared_tree

    destination = _assert_retained(value)
    source = _scope().source
    item, *_ = enrolled()
    marker_relative = _safe_relative(item.get('marker', 'download_summary.json'))
    marker = destination / marker_relative
    marker_sha = relocation.digest(marker, no_follow=True)
    destination_identity = inode(destination)
    placeholder_identity = [ROOT.stat().st_dev, value['placeholder_inode']]
    original_lock_identity = [ROOT.stat().st_dev, value['original_lock_inode']]
    proc_fd = os.open('/proc', os.O_RDONLY | os.O_DIRECTORY)
    seen, observations, attach_retries, stat_retries = set(), [], {}, {}
    try:
        for _ in range(5):
            try:
                namespaces = providers.namespaces(with_roots=True)
            except ProcessLookupError:
                # A process can exit while its /proc namespace is being
                # pinned. Discard that census and repeat it; never infer that
                # the namespace was mounted or suppress unrelated OS errors.
                continue
            try:
                if not namespaces:
                    raise SnapshotError('source handoff namespace census is empty')
                pending = {name: fds for name, fds in namespaces.items() if name not in seen}
                if not pending:
                    volume.check()
                    return observations
                for ns_inode, (ns_fd, root_fd) in pending.items():
                    with prepared_tree(destination, ['rw', 'relatime']) as tree_fd:
                        info = os.fstat(tree_fd)
                        if [info.st_dev, info.st_ino] != destination_identity:
                            raise SnapshotError('detached E source inode differs')
                        request = {'repository_root': str(ROOT), 'repository_identity': inode(ROOT),
                                   'target': str(source), 'namespace_inode': ns_inode,
                                   'namespace_fd': ns_fd, 'namespace_root_fd': root_fd,
                                   'proc_fd': proc_fd, 'tree_fd': tree_fd,
                                   'destination_identity': destination_identity,
                                   'placeholder_identity': placeholder_identity,
                                   'original_lock_identity': original_lock_identity,
                                   'marker_relative': marker_relative.as_posix(),
                                   'marker_sha256': marker_sha}
                        program = ('import scripts.workspace_source_handoff_namespace as h; h.main()')
                        # The namespace helper and its imports use the stdlib
                        # and this checkout only. Avoid site initialization for
                        # every source/namespace pair while retaining a fresh
                        # process, the pinned FDs and all mount/readback checks.
                        child = subprocess.run([sys.executable, '-S', '-c', program], cwd=ROOT,
                            pass_fds=(ns_fd, root_fd, proc_fd, tree_fd), input=json.dumps(request),
                            capture_output=True, text=True, timeout=90)
                        if child.returncode:
                            error = RuntimeError('source namespace handoff failed: ' + child.stderr[-1800:])
                            if providers.vanished_namespace_after_missing_attach(ns_inode, error):
                                continue
                            # A short-lived namespace can still be visible in
                            # the first re-census while move_mount returns
                            # ENOENT. Re-pin it on the next complete census;
                            # persistent or unrelated failures still block.
                            missing_attach = ('FileNotFoundError: [Errno 2]' in str(error)
                                and f'provider alias attach failed: {source}' in str(error))
                            if missing_attach and attach_retries.get(ns_inode, 0) < 2:
                                attach_retries[ns_inode] = attach_retries.get(ns_inode, 0) + 1
                                continue
                            raise error
                        observed = json.loads(child.stdout)
                        if (observed.get('verified') is not True or observed.get('namespace_inode') != ns_inode
                                or observed.get('destination_identity') != destination_identity):
                            raise SnapshotError('source namespace readback differs')
                        # A second process independently observes the same
                        # bound inode after the helper leaves that namespace.
                        probe = ('import os,ctypes,json,sys\nq=json.load(sys.stdin);c=ctypes.CDLL(None,use_errno=True)\n'
                                 'assert c.setns(q["namespace_fd"],0)==0\n'
                                 'os.fchdir(q["namespace_root_fd"]);os.chroot(".");os.chdir("/")\n'
                                 's=os.stat(q["path"]);print(str(s.st_dev)+":"+str(s.st_ino))\n')
                        # This independent stat probe imports only stdlib. It
                        # runs once per active source and mount namespace, so
                        # avoid repeated environment/site initialization while
                        # keeping the second process and pinned FDs unchanged.
                        readback = subprocess.run([sys.executable, '-S', '-c', probe], cwd=ROOT,
                            pass_fds=(ns_fd, root_fd),
                            input=json.dumps({'namespace_fd': ns_fd, 'namespace_root_fd': root_fd,
                                              'path': str(source)}),
                            capture_output=True, text=True, check=True, timeout=15).stdout.strip()
                        if readback != ':'.join(map(str, destination_identity)):
                            # A short-lived namespace may change between the
                            # mount helper and this second process. Re-census
                            # and repeat both observations, but never accept
                            # the mismatching readback itself.
                            if stat_retries.get(ns_inode, 0) < 2:
                                stat_retries[ns_inode] = stat_retries.get(ns_inode, 0) + 1
                                continue
                            raise SnapshotError(f'independent source namespace stat differs '
                                                f'(namespace={ns_inode}, observed={readback})')
                        observations.append({**observed, 'independent_stat': readback})
                        seen.add(ns_inode)
            finally:
                for fds in namespaces.values():
                    for fd in fds:
                        os.close(fd)
        raise BlockingIOError('source namespace census did not converge')
    finally:
        os.close(proc_fd)


def _source_state(value):
    """Identify the two sides by inode, including an interrupted exchange."""
    source, backup = _scope().source, _scope().backup
    if backup.is_symlink() or source.is_symlink():
        raise SnapshotError('source handoff roots cannot be symlinks')
    backup_identity = inode(backup) if backup.exists() else None
    source_identity = inode(source) if source.exists() else None
    original = [ROOT.stat().st_dev, value['original_inode']]
    # A crash can leave an empty or lock-only placeholder before it was
    # journalled. _prepare_placeholder validates it before reuse.
    if source_identity == original and value['state'] == 'preparing':
        return 'original'
    placeholder = value.get('placeholder_inode')
    placeholder_identity = [ROOT.stat().st_dev, placeholder]
    if (placeholder is not None and source_identity == original
            and backup_identity == placeholder_identity):
        return 'prepared'
    if backup_identity == original:
        if provider_workspace.exact_mount(source, Path(value['destination'])):
            return 'mounted'
        if source_identity == placeholder_identity:
            return 'exchanged'
    raise SnapshotError('source handoff root identities are inconsistent')


def _owner_clear(item):
    from scripts import run_workspace_source_staging as queue
    from stockagent.data_sync.materialized_cache import process_references_many
    queue.check_known_running_owners(item)
    references = process_references_many([_scope().source])
    if references:
        raise BlockingIOError('source has live references; wait for the original owner')


def _prepare_placeholder(value):
    """A single hardlinked original lock prevents an unsafe C-side fallback."""
    source, backup = _scope().source, _scope().backup
    if not backup.exists():
        backup.mkdir(mode=stat.S_IMODE(source.stat().st_mode))
        os.chown(backup, source.stat().st_uid, source.stat().st_gid)
        _fsync_directory(ROOT)
    if (backup.is_symlink() or not backup.is_dir() or backup.stat().st_dev != ROOT.stat().st_dev
            or {entry.name for entry in backup.iterdir()} - {'.download.lock'}):
        raise SnapshotError('source handoff placeholder contains unexpected data')
    link = backup / '.download.lock'
    if not link.exists():
        os.link(source / '.download.lock', link, follow_symlinks=False)
        _fsync_directory(backup)
    if link.is_symlink() or inode(link) != inode(source / '.download.lock'):
        raise SnapshotError('source handoff placeholder does not hold the original lock')
    value['placeholder_inode'] = backup.stat().st_ino
    value['lock_signature'] = relocation.signature(source / '.download.lock')
    value['state'] = 'prepared'
    save(value)


def _same_copy_proof(value, proof):
    for key in ('files', 'logical_bytes', 'copy_result_sha256', 'report_sha256', 'marker_sha256',
                'handoff_verification_mode', 'new_payload_readback', 'source_generation_overrides',
                'internal_symlink_closure'):
        if value.get(key) != proof.get(key):
            raise SnapshotError('source handoff fixed proof changed')


def _activate_prepared_locked(item, value, *, mutation_fence=None):
    """Exchange the prepared roots while their reviewed writer locks remain held."""
    source, backup = _scope().source, _scope().backup
    lock = source / '.download.lock'
    if lock.stat().st_ino != value['original_lock_inode'] or relocation.signature(lock) != value['lock_signature']:
        raise SnapshotError('original collector lock changed before exchange')
    _owner_clear(item)
    if _scope().name == YAHOO_NAME:
        _validate_yahoo_enrollment()
    if _scope().name == PEPPERSTONE_NAME:
        _validate_pepperstone_enrollment()
    if _scope().name in RETAINED_LOCK_SOURCES:
        _validate_retained_enrollment(_scope().name)
    if _source_state(value) != 'prepared':
        raise SnapshotError('source roots changed before atomic exchange')
    if (_scope().name in HEAVY_SOURCES
            and not binance_handoff_window_allowed(for_start=False)):
        raise BlockingIOError('tw_day_trade_priority_window')
    if mutation_fence is not None:
        mutation_fence.check(allow_lock_attrib=True)
        if relocation.signature(lock) != value['lock_signature']:
            raise BlockingIOError('source lock changed during the mutation fence')
    exchange(source, backup)
    if mutation_fence is not None:
        try:
            mutation_fence.check(allow_lock_attrib=True, allow_root_move=True)
            if relocation.signature(backup/'.download.lock') != value['lock_signature']:
                raise BlockingIOError('source lock changed during the atomic exchange')
        except BlockingIOError:
            # No mount was attached yet. Roll the exact exchange back only if
            # the placeholder still contains solely its original lock name.
            if (source.is_symlink() or not source.is_dir()
                    or {entry.name for entry in source.iterdir()} != {'.download.lock'}):
                value['state'] = 'exchanged'
                save(value)
                raise SnapshotError('source placeholder acquired unexpected data during handoff')
            exchange(source, backup)
            value['state'] = 'prepared'
            save(value)
            raise
    immutable_placeholder(source)
    value['state'] = 'exchanged'
    save(value)
    observations = mount_everywhere(value)
    value.update(state='active', last_namespace_readback=observations,
                 activated_at_epoch=time.time())
    save(value)
    return {'state': 'active', 'namespaces': len(observations), 'c_source_retained': True}


@contextmanager
def _futures_publication_owner():
    """Fence the same-dataset publisher without blocking unrelated D retirement.

    This handoff neither reads nor mutates D objects. Every futures publisher
    owns this exclusive dataset lock while it reads the parent source. The
    unrelated global retention lock is therefore not needed here.
    """
    from stockagent.data_sync.cold_primary import _check_d_primary_mount
    from stockagent.data_sync.packed_snapshots import PackedPublicationBusy, _try_publication_lock
    cold = COLD_PRIMARY
    _check_d_primary_mount(cold)
    try:
        with ExitStack() as owners:
            owners.enter_context(_try_publication_lock(cold/'.local-state/locks/publish-tw-index-futures.lock'))
            yield
    except PackedPublicationBusy as error:
        raise BlockingIOError('futures publication owner is busy') from error


@contextmanager
def _market_publication_owner(dataset):
    """Fence the canonical public publisher while its source is exchanged."""
    from stockagent.data_sync.cold_primary import _check_d_primary_mount
    from stockagent.data_sync.packed_snapshots import PackedPublicationBusy, _try_publication_lock
    _check_d_primary_mount(COLD_PRIMARY)
    try:
        if dataset not in {'bybit', 'yahoo-market', 'forex-pepperstone', 'taifex-public-history',
                           'taifex-rule-history', 'tw-index-options-daily'}:
            raise SnapshotError('market publication owner is not enrolled')
        with _try_publication_lock(COLD_PRIMARY/'.local-state/locks'/f'publish-{dataset}.lock'):
            yield
    except PackedPublicationBusy as error:
        raise BlockingIOError('market publication owner is busy') from error


@contextmanager
def _yahoo_publication_owner():
    with _market_publication_owner('yahoo-market'):
        yield


@contextmanager
def _mutation_fence(source, destination, evidence):
    name = _scope().name
    if name not in {FUTURES_725_NAME, FUTURES_765_NAME, YAHOO_NAME, PEPPERSTONE_NAME, BYBIT_NAME, *RETAINED_LOCK_SOURCES}:
        with nullcontext(None) as fence:
            yield fence
        return
    from stockagent.data_sync.source_mutation_fence import SourceMutationFence
    with ExitStack() as owners:
        if name == BYBIT_NAME:
            owners.enter_context(_market_publication_owner('bybit'))
        elif name == YAHOO_NAME:
            owners.enter_context(_yahoo_publication_owner())
        elif name == PEPPERSTONE_NAME:
            owners.enter_context(_market_publication_owner('forex-pepperstone'))
        elif name in {FUTURES_725_NAME, FUTURES_765_NAME}:
            owners.enter_context(_futures_publication_owner())
        elif name == TAIFEX_PUBLIC_NAME:
            for dataset in ('taifex-public-history', 'taifex-rule-history'):
                owners.enter_context(_market_publication_owner(dataset))
        elif name == OPTIONS_RAW_NAME:
            owners.enter_context(_market_publication_owner('tw-index-options-daily'))
        options = {'max_directories': 160000} if name == TAIFEX_PUBLIC_NAME else {}
        setup = _RecheckProgress('initial', 0, 0) if name == TAIFEX_PUBLIC_NAME else None
        if setup:
            options['progress'] = setup.advance_fence
        try:
            with SourceMutationFence(source, destination, evidence/'files.sqlite',
                                     guard=_source_work_window_guard(), **options) as fence:
                if setup:
                    setup.finish('checked')
                yield fence
        except BaseException as error:
            if setup and setup.value['state'] == 'running':
                setup.finish('waiting' if isinstance(error, BlockingIOError) else 'blocked')
            raise


def _verify_with_fence(item, workspace, fence, **kwargs):
    if fence is not None:
        kwargs['mutation_fence'] = fence
    return verify_current_copy(item, workspace, **kwargs)


def handoff(*, boot=False):
    """Advance the enrolled source once; all payloads remain in their old roots.

    Boot with no journal is deliberately a no-op. Once exchanged, both the
    boot owner and the timer reacquire the same C/E writer locks and repair
    only the enrolled namespace aliases.  No C retirement is authorized.
    """
    from scripts import run_workspace_source_staging as queue
    source, backup = _scope().source, _scope().backup
    CONTROL.mkdir(mode=0o700, parents=True, exist_ok=True)
    if CONTROL.is_symlink() or CONTROL.resolve(strict=True) != CONTROL:
        raise SnapshotError('source handoff owner root is redirected')
    with (CONTROL / 'owner.lock').open('a+') as owner:
        fcntl.flock(owner, fcntl.LOCK_EX | fcntl.LOCK_NB)
        value = read_journal()
        if boot and value is None:
            return {'state': 'not_enrolled'}
        if _scope().name == OKX_NAME and value is None:
            _validate_okx_enrollment()
        if _scope().name == NEWS_NAME and value is None:
            _validate_news_enrollment()
        if _scope().name == NEWS_BODIES_NAME and value is None:
            _validate_news_bodies_enrollment()
        if _scope().name == HFT_NAME and value is None:
            _validate_hft_enrollment()
        if _scope().name == MINUTE_NAME and value is None:
            _validate_minute_enrollment()
        if (_scope().name == MINUTE_RAW_NAME
                and (value is None or value.get('state') in {'preparing', 'prepared'})):
            _validate_minute_raw_enrollment()
        if (_scope().name == BYBIT_NAME
                and (value is None or value.get('state') in {'preparing', 'prepared'})):
            _validate_bybit_enrollment()
        if _scope().name == FUTURES_NAME and value is None:
            _validate_futures_enrollment()
        if _scope().name == FUTURES_725_NAME and value is None:
            _validate_futures_725_enrollment()
        if _scope().name == FUTURES_765_NAME and value is None:
            _validate_futures_765_enrollment()
        if _scope().name == YAHOO_NAME and (value is None or value.get('state') in {'preparing', 'prepared'}):
            _validate_yahoo_enrollment()
        if _scope().name == PEPPERSTONE_NAME and (value is None or value.get('state') in {'preparing', 'prepared'}):
            _validate_pepperstone_enrollment()
        if (_scope().name in RETAINED_LOCK_SOURCES
                and (value is None or value.get('state') in {'preparing', 'prepared'})):
            _validate_retained_enrollment(_scope().name)
        item, workspace, destination, evidence, _ = enrolled()
        volume.check()
        if value is None:
            if (source.is_symlink() or not source.is_dir() or source.resolve(strict=True) != source
                    or source.stat().st_dev != ROOT.stat().st_dev
                    or provider_workspace.exact_mount(source, destination)
                    or backup.exists() or backup.is_symlink()):
                raise SnapshotError('source is not the expected original C root')
            with ExitStack() as locks:
                _lock_writer_roots(locks, source, destination)
                _owner_clear(item)
                fence = locks.enter_context(_mutation_fence(source, destination, evidence))
                proof = _verify_with_fence(item, workspace, fence,
                    progress_pass='initial' if _scope().name != NAME else None)
                value = {'contract': CONTRACT, 'state': 'preparing',
                         'source': str(source), 'backup': str(backup),
                         'destination': str(destination), 'evidence': str(evidence),
                         'workspace': str(workspace), 'original_inode': source.stat().st_ino,
                         'original_lock_inode': (source / '.download.lock').stat().st_ino,
                         'destination_inode': destination.stat().st_ino,
                         'created_at_epoch': time.time(), **proof}
                save(value)
                _prepare_placeholder(value)
                # No writer lock was released after the complete fixed-copy
                # check. Repeating the same multi-pass scan here cannot see a
                # legitimate owner change; a resumed journal still rechecks.
                return _activate_prepared_locked(item, value, mutation_fence=fence)
        state = _source_state(value)
        if boot and state in {'original', 'prepared'}:
            # The original source remains visible. The collector's guard keeps
            # writes paused until the timer resumes the fixed handoff.
            return {'state': 'awaiting_handoff', 'source_unchanged': True}
        if state == 'original':
            with ExitStack() as locks:
                _lock_writer_roots(locks, source, destination)
                fence = locks.enter_context(_mutation_fence(source, destination, evidence))
                if ((source / '.download.lock').stat().st_ino != value['original_lock_inode']):
                    raise SnapshotError('original collector lock was replaced')
                placeholder_lock = backup / '.download.lock'
                signature = None
                if placeholder_lock.exists():
                    if placeholder_lock.is_symlink() or inode(placeholder_lock) != inode(source / '.download.lock'):
                        raise SnapshotError('interrupted placeholder lock differs')
                    signature = relocation.signature(source / '.download.lock')
                _same_copy_proof(value, _verify_with_fence(item, workspace, fence, lock_signature=signature,
                    progress_pass='preexchange' if _scope().name != NAME else None))
                _owner_clear(item)
                _prepare_placeholder(value)
                if fence is not None:
                    fence.check(allow_lock_attrib=True)
            state = 'prepared'
        if state == 'prepared':
            with ExitStack() as locks:
                _lock_writer_roots(locks, source, destination)
                fence = locks.enter_context(_mutation_fence(source, destination, evidence))
                if ((source / '.download.lock').stat().st_ino != value['original_lock_inode']):
                    raise SnapshotError('original collector lock was replaced')
                _same_copy_proof(value, _verify_with_fence(item, workspace, fence, lock_signature=value['lock_signature'],
                    progress_pass='preexchange' if _scope().name != NAME else None))
                return _activate_prepared_locked(item, value, mutation_fence=fence)
        if state in {'exchanged', 'mounted'}:
            with ExitStack() as locks:
                _lock_writer_roots(locks, backup, destination)
                _assert_retained(value)
                if state == 'exchanged':
                    immutable_placeholder(source)
                observations = mount_everywhere(value)
                value.update(state='active', last_namespace_readback=observations,
                             checked_at_epoch=time.time())
                save(value)
                return {'state': 'active', 'namespaces': len(observations), 'c_source_retained': True}
        raise SnapshotError('source handoff state cannot advance')


