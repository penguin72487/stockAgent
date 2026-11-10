#!/usr/bin/env python3
"""Attach one already-validated E source in a pinned consumer namespace.

This helper receives only open mount/namespace FDs and exact inode identities.
It never copies or removes data, and it refuses to hide an unexpected mount or
anything other than the enrolled C-side lock placeholder.
"""
from __future__ import annotations

import json
import os
from pathlib import Path
import stat
import sys

from scripts import provider_workspace_namespace as common
from stockagent.data_sync.transport_namespace_handoff import MountAttributes, attach_tree, mount_api
from stockagent.data_sync.workspace_relocation import digest


def attach(request):
    common._PROC_FD = request['proc_fd']
    root = Path(request['repository_root'])
    target = Path(request['target'])
    reviewed_body_target = target == root / 'data_news_fulltext/bodies'
    reviewed_hft_target = target == root / 'data_tw_microstructure/hft_dataset'
    reviewed_minute_target = target == root / 'data_tw_minute/research_dataset'
    reviewed_minute_raw_target = target == root / 'data_tw_minute/shioaji_1m'
    reviewed_bybit_target = target == root / 'service_data/stockagent-live/data_bybit'
    reviewed_futures_target = target == root / 'data_tw_index_futures/training_prepared'
    reviewed_futures_725 = target == root / 'data_tw_index_futures/training_prepared/tw_futures_universe725_20261025_v2'
    reviewed_futures_765 = target == root / 'data_tw_index_futures/training_prepared/tw_futures_universe765_20261027_v1'
    reviewed_options_raw = target == root / 'data_tw_index_options_daily/raw'
    reviewed_nested_target = (reviewed_body_target or reviewed_hft_target or reviewed_minute_target or reviewed_minute_raw_target or reviewed_bybit_target
                              or reviewed_futures_target or reviewed_futures_725 or reviewed_futures_765 or reviewed_options_raw)
    if (not root.is_absolute() or root.resolve(strict=True) != root
            or common.identity(root) != request['repository_identity']
            or (reviewed_nested_target and (target.parent.is_symlink()
                or target.parent.resolve(strict=True) != target.parent))
            or (not reviewed_nested_target and (target.parent != root
            or target.name not in {'data_crypto_etf', 'data_coinmetrics_community',
                'data_crypto_historical_public', 'data_dune_crypto',
                'data_keyed_public_catalogs', 'data_crypto_reference', 'data_forex_frankfurter',
                'data_binance', 'data_binance_archive', 'data_okx', 'data_parquet',
                'data_free_public', 'data_public_economic', 'data_news', 'data_yahoo', 'data_peperstone',
                'data_cftc_legacy', 'data_fred_crypto_macro', 'data_taifex_public_history'}))):
        raise RuntimeError('source handoff repository or enrolled target differs')
    namespace = os.stat('self/ns/mnt', dir_fd=request['proc_fd']).st_ino
    if not request.get('isolated_probe') and namespace != request['namespace_inode']:
        raise RuntimeError('source handoff namespace identity changed')
    tree = request['tree_fd']
    info = os.fstat(tree)
    if not stat.S_ISDIR(info.st_mode) or [info.st_dev, info.st_ino] != request['destination_identity']:
        raise RuntimeError('source handoff detached tree identity differs')
    mounts = common.mounts()
    if str(target) in mounts:
        if common.identity(target) != request['destination_identity']:
            raise RuntimeError('source handoff found a different existing mount')
    else:
        if target.resolve(strict=True) != target or common.identity(target) != request['placeholder_identity']:
            raise RuntimeError('source handoff placeholder identity differs')
        if any(name.startswith(str(target) + '/') for name in mounts):
            raise RuntimeError('source handoff placeholder contains a submount')
        if {p.name for p in target.iterdir()} != {'.download.lock'}:
            raise RuntimeError('source handoff placeholder contains unexpected names')
        lock = target / '.download.lock'
        if (lock.is_symlink() or not stat.S_ISREG(lock.lstat().st_mode)
                or common.identity(lock) != request['original_lock_identity']):
            raise RuntimeError('source handoff placeholder lock inode differs')
        restrictions = os.statvfs(target).f_flag & 0xF
        if restrictions:
            attrs = MountAttributes(restrictions, 0, 0, 0)
            import ctypes
            mount_api('mount_setattr', (tree, b'', 0x1000, ctypes.byref(attrs), ctypes.sizeof(attrs)),
                      (ctypes.c_int, ctypes.c_char_p, ctypes.c_uint,
                       ctypes.POINTER(MountAttributes), ctypes.c_size_t))
        try:
            attach_tree(tree, target)
        except OSError as error:
            raise OSError(error.errno, f'provider alias attach failed: {target}') from error
    if str(target) not in common.mounts() or common.identity(target) != request['destination_identity']:
        raise RuntimeError('source handoff mounted inode differs')
    expected_marker = ('annual/2001_opt.zip' if reviewed_options_raw
                       else 'financial_all_twd_v8/requested_universe.csv' if reviewed_futures_765
                       else 'requested_725.csv' if reviewed_futures_725
                       else 'tx_contract_days_audit_projection_20261030_v1/manifest.json' if reviewed_futures_target
                       else '20261027/20261027T171812670228Z-349dc38d.parquet' if reviewed_body_target
                       else 'manifest.json' if reviewed_hft_target or reviewed_minute_target
                       else '1m/download_summary.json' if target.name == 'data_okx'
                       else 'manifest.json' if target.name in {'data_news', 'data_cftc_legacy', 'data_taifex_public_history'}
                       else 'README.md' if target.name == 'data_parquet' else 'download_summary.json')
    if request.get('marker_relative', 'download_summary.json') != expected_marker:
        raise RuntimeError('source handoff marker scope differs')
    marker = target / expected_marker
    if marker.is_symlink() or digest(marker, no_follow=True) != request['marker_sha256']:
        raise RuntimeError('source handoff marker readback differs')
    return {'namespace_inode': namespace, 'target': str(target), 'verified': True,
            'destination_identity': request['destination_identity']}


def main():
    raw = sys.stdin.buffer.read(32769)
    if len(raw) > 32768:
        raise ValueError('source handoff namespace request exceeds its bound')
    request = json.loads(raw)
    if os.fstat(request['namespace_fd']).st_ino != request['namespace_inode']:
        raise RuntimeError('source handoff pinned namespace FD differs')
    common.enter(request)
    print(json.dumps(attach(request)))


if __name__ == '__main__':
    main()
