"""A real, isolated mount exchange and boot repair for the reviewed source."""
from __future__ import annotations

import os
from pathlib import Path
import shutil
import subprocess
import sys
import textwrap

import pytest


@pytest.mark.parametrize('source_name,root_name', [
    ('coinmetrics-community-e-primary-20261104-v1', 'data_coinmetrics_community'),
    ('dune-crypto-e-primary-20261104-v1', 'data_dune_crypto'),
    ('forex-frankfurter-e-primary-20261104-v1', 'data_forex_frankfurter'),
    ('binance-market-e-primary-20261104-v1', 'data_binance'),
    ('binance-archive-e-primary-20261104-v1', 'data_binance_archive'),
    ('bybit-source-stream-e-20261108-v1', 'service_data/stockagent-live/data_bybit'),
    ('okx-e-primary-20261104-v1', 'data_okx'),
    ('legacy-parquet-e-primary-20261104-v1', 'data_parquet'),
    ('free-public-context-e-primary-20261104-v1', 'data_free_public'),
    ('public-economic-e-primary-20261104-v1', 'data_public_economic'),
    ('news-training-local-e-primary-20261104-v1', 'data_news'),
    ('news-fulltext-bodies-e-primary-20261104-v1', 'data_news_fulltext/bodies'),
    ('tw-microstructure-train-e-primary-20261104-v1', 'data_tw_microstructure/hft_dataset'),
    ('tw-minute-train-stream-e-20261108-v1', 'data_tw_minute/research_dataset'),
    ('tw-index-options-raw-stream-e-20261108-v1', 'data_tw_index_options_daily/raw'),
    ('tw-futures-prepared-e-primary-20261104-v1', 'data_tw_index_futures/training_prepared'),
    ('tw-futures-universe725-e-projection-20261108-v1',
     'data_tw_index_futures/training_prepared/tw_futures_universe725_20261025_v2'),
    ('tw-futures-universe765-e-projection-20261108-v1',
     'data_tw_index_futures/training_prepared/tw_futures_universe765_20261027_v1'),
    ('yahoo-market-e-primary-20261104-v1', 'data_yahoo'),
    ('forex-pepperstone-e-primary-20261104-v1', 'data_peperstone'),
    ('cftc-legacy-pre2000-e-primary-20261104-v1', 'data_cftc_legacy'),
    ('fred-crypto-macro-e-primary-20261104-v1', 'data_fred_crypto_macro'),
    ('taifex-public-history-e-primary-20261104-v1', 'data_taifex_public_history'),
])
def test_reviewed_handoff_and_boot_restore_in_isolated_mount_namespace(tmp_path, source_name, root_name):
    if os.geteuid() != 0 or not shutil.which('unshare'):
        pytest.skip('isolated kernel mount test requires root and unshare')
    root = Path(__file__).resolve().parents[1]
    script = textwrap.dedent('''
        import hashlib, json, os, shutil, sqlite3, subprocess, sys, time
        from pathlib import Path
        from scripts import run_workspace_source_staging as queue
        from scripts import relocate_provider_workspace as provider
        from stockagent.data_sync import workspace_relocation as relocation
        from stockagent.data_sync import workspace_source_handoff as handoff
        from stockagent.data_sync.desync_snapshots import SnapshotError

        base = Path(sys.argv[1]); repo = base / 'repo'; repo.mkdir()
        source = repo / sys.argv[3]; source.mkdir(parents=True)
        if sys.argv[3] == 'data_news_fulltext/bodies':
            (source.parent / 'owner.lock').write_bytes(b'')
        if sys.argv[3] not in {'data_binance_archive', 'data_okx', 'data_parquet', 'data_free_public', 'data_public_economic', 'data_news', 'data_news_fulltext/bodies', 'data_tw_microstructure/hft_dataset', 'data_yahoo', 'data_peperstone', 'data_cftc_legacy', 'data_fred_crypto_macro', 'data_taifex_public_history', 'data_tw_index_options_daily/raw', 'service_data/stockagent-live/data_bybit',
                               'data_tw_index_futures/training_prepared/tw_futures_universe725_20261025_v2',
                               'data_tw_index_futures/training_prepared/tw_futures_universe765_20261027_v1'}:
            (source / '.download.lock').write_bytes(b'')
        is_725 = sys.argv[3] == 'data_tw_index_futures/training_prepared/tw_futures_universe725_20261025_v2'
        is_765 = sys.argv[3] == 'data_tw_index_futures/training_prepared/tw_futures_universe765_20261027_v1'
        is_taifex = sys.argv[3] == 'data_taifex_public_history'
        is_options_raw = sys.argv[3] == 'data_tw_index_options_daily/raw'
        marker = ('financial_all_twd_v8/requested_universe.csv' if is_765 else
                  'requested_725.csv' if is_725 else
                  handoff.FUTURES_MARKER if sys.argv[3] == 'data_tw_index_futures/training_prepared' else
                  '1m/download_summary.json' if sys.argv[3] == 'data_okx' else
                  handoff.NEWS_BODIES_MARKER if sys.argv[3] == 'data_news_fulltext/bodies' else
                  'manifest.json' if sys.argv[3] == 'data_tw_microstructure/hft_dataset' else
                  'manifest.json' if sys.argv[3] == 'data_tw_minute/research_dataset' else
                  'manifest.json' if sys.argv[3] == 'data_news' else
                  'manifest.json' if sys.argv[3] == 'data_cftc_legacy' else
                  'annual/2001_opt.zip' if is_options_raw else
                  'manifest.json' if is_taifex else
                  'README.md' if sys.argv[3] == 'data_parquet' else 'download_summary.json')
        (source / ('README.md' if sys.argv[3] == 'data_parquet' else 'download_summary.json')).write_text('{"generation":1}')
        (source / 'original.bin').write_bytes(b'original-source-content')
        if is_options_raw:
            (source/'annual').mkdir()
            (source/marker).write_bytes(b'fixed-raw')
            (source.parent/'state').mkdir()
            (source.parent/'state/registered_auxiliary_daily.lock').write_bytes(b'')
            (source.parent/'txo_final_settlement_history.parquet').write_bytes(b'live-settlement')
        if sys.argv[3] in {'data_cftc_legacy', 'data_fred_crypto_macro', 'data_taifex_public_history'}:
            (source/'state').mkdir()
            (source/'state/collector.lock').write_bytes(b'')
            if is_taifex:
                (source/'rules/state').mkdir(parents=True)
                (source/'rules/state/collector.lock').write_bytes(b'')
            (source/marker).write_text('{"generation":1}')
        if is_725:
            (source / marker).write_text('{"generation":1}')
        if is_765:
            (source / marker).parent.mkdir()
            (source / marker).write_text('{"generation":1}')
            (source / 'internal-link').symlink_to('original.bin')
            (source.parent/'factorized-sibling').write_text('unrelated')
        if sys.argv[3] == 'data_tw_index_futures/training_prepared':
            (source / marker).parent.mkdir()
            (source / marker).write_text('{"generation":1}')
            (source / 'internal-link').symlink_to('original.bin')
        if sys.argv[3] == 'data_news_fulltext/bodies':
            (source / marker).parent.mkdir()
            (source / marker).write_bytes(b'fixed-original-article')
        if sys.argv[3] in {'data_tw_microstructure/hft_dataset', 'data_tw_minute/research_dataset'}:
            (source / 'manifest.json').write_text('{"generation":1}')
        if sys.argv[3] == 'data_news':
            (source / 'manifest.json').write_text('{"generation":1}')
            (source / '.build.lock').write_bytes(b'')
        if sys.argv[3] == 'data_public_economic':
            for provider_name in ('bea', 'census', 'moi'):
                (source / ('.' + provider_name + '.lock')).write_bytes(b'')
        if sys.argv[3] == 'data_forex_frankfurter':
            (source / 'official_v2').mkdir()
            (source / 'official_v2/.download.lock').write_bytes(b'')
        if sys.argv[3] == 'data_binance':
            for grain in ('1m', 'daily'):
                (source / grain).mkdir()
                (source / grain / '.download.lock').write_bytes(b'')
        if sys.argv[3] == 'service_data/stockagent-live/data_bybit':
            (repo / 'data_bybit').symlink_to(source, target_is_directory=True)
            global_lock = repo / 'artifacts/daily_downloader/bybit_source_publish.lock'
            global_lock.parent.mkdir(parents=True)
            global_lock.write_bytes(b'')
            for grain in ('1m', 'daily'):
                (source / grain).mkdir()
                (source / grain / '.download.lock').write_bytes(b'')
        if sys.argv[3] == 'data_okx':
            for grain in ('1m', 'daily'):
                (source / grain).mkdir()
                (source / grain / '.download.lock').write_bytes(b'')
            (source / '1m/.quality_priority.lock').write_bytes(b'')
            (source / '1m/download_summary.json').write_text('{"generation":1}')
        workspace = base / 'workspace'; workspace.mkdir()
        destination = workspace / 'source'; shutil.copytree(source, destination, symlinks=True)
        evidence = workspace / 'evidence'; evidence.mkdir()
        report_path = workspace / 'report.json'; report_path.write_text('{}')
        is_futures = sys.argv[3] == 'data_tw_index_futures/training_prepared'
        members = list(relocation.walk(source, preserve_symlinks=is_futures or is_765))
        files = [row for row in members if row[1] == 'file']
        (evidence / 'intent.json').write_text(json.dumps({
            'contract':'workspace_scoped_relocation_v1' if is_futures or is_725 or is_765 or is_taifex else 'workspace_exact_relocation_v1', 'source':str(source),
            'destination':str(destination), 'source_root_signature':relocation.signature(source),
            'entries':len(members), **({'preserve_symlinks': True, 'excluded_subtrees': []} if is_futures or is_725 or is_765 or is_taifex else {})}))
        with sqlite3.connect(evidence / 'files.sqlite') as db:
            db.execute('CREATE TABLE files (path TEXT PRIMARY KEY,kind TEXT,sig TEXT,sha TEXT,bytes INTEGER,state TEXT)')
            for name, kind, signature in members:
                digest = (relocation.digest(source / name, no_follow=True) if kind == 'file' else
                          hashlib.sha256(os.fsencode(os.readlink(source / name))).hexdigest() if kind == 'symlink' else None)
                db.execute('INSERT INTO files VALUES (?,?,?,?,?,?)',
                    (name, kind, json.dumps(signature), digest, 0 if kind == 'symlink' else signature[3], 'verified'))
        copy = {'state':'verified'}
        if sys.argv[3] in {'data_parquet', 'data_free_public', 'data_public_economic', 'data_news', 'data_news_fulltext/bodies', 'data_tw_microstructure/hft_dataset', 'data_yahoo', 'data_peperstone', 'data_cftc_legacy', 'data_fred_crypto_macro'}:
            copy = {'contract':'workspace_exact_relocation_v1',
                    'state':'exact_destination_readback_verified',
                    'all_source_sha256_and_destination_readback_verified':True,
                    'source_namespace_unchanged':True, 'source_files_deleted':0,
                    'files':len(files), 'logical_bytes':sum(row[2][3] for row in files),
                    'observed_at_epoch':time.time()}
        if sys.argv[3] in {'data_tw_minute/research_dataset', 'service_data/stockagent-live/data_bybit', 'data_tw_index_options_daily/raw'}:
            copy = {'contract':'stockagent.source_stream_staging.v1',
                    'state':'source_sha256_streamed_no_destination_readback',
                    'source_sha256_streamed':True, 'destination_independent_readback':False,
                    'all_source_sha256_and_destination_readback_verified':False,
                    'source_namespace_unchanged':True, 'source_files_deleted':0,
                    'files':len(files), 'logical_bytes':sum(row[2][3] for row in files),
                    'observed_at_epoch':time.time()}
        if is_futures or is_725 or is_765 or is_taifex:
            copy = {'contract':'workspace_scoped_relocation_v1',
                    'state':'exact_destination_readback_verified',
                    'selected_source_sha256_and_destination_readback_verified':True,
                    'all_source_sha256_and_destination_readback_verified':True, 'excluded_subtrees':[],
                    'source_namespace_unchanged':True, 'source_files_deleted':0,
                    'files':len(files), 'logical_bytes':sum(row[2][3] for row in files),
                    'observed_at_epoch':time.time()}
        (evidence / 'copy-result.json').write_text(json.dumps(copy))
        report = {'files':len(files),'logical_bytes':sum(row[2][3] for row in files),
                  'relocation_evidence_sha256':hashlib.sha256((evidence/'copy-result.json').read_bytes()).hexdigest(),
                  'source_marker_sha256':hashlib.sha256((source/marker).read_bytes()).hexdigest(),
                  **({'symlink_names_preserved':1} if is_futures or is_765 else {})}
        item = {'name':sys.argv[2],'source':sys.argv[3], 'marker':marker}
        handoff.ROOT = repo; handoff.CONTROL = base / 'control'
        if sys.argv[3] == 'data_okx':
            handoff.CONTROL.mkdir()
            body = {'contract':'stockagent.okx_source_handoff_enrollment.v1', 'name':handoff.OKX_NAME,
                    'source':'data_okx', 'marker':'1m/download_summary.json', 'preserve_c_original':True}
            body['identity_sha256'] = hashlib.sha256(json.dumps(body, sort_keys=True, separators=(',', ':')).encode()).hexdigest()
            (handoff.CONTROL / 'okx-enrollment.json').write_text(json.dumps(body))
        if sys.argv[3] == 'data_news':
            handoff.CONTROL.mkdir()
            body = {'contract':'stockagent.news_source_handoff_enrollment.v1', 'name':handoff.NEWS_NAME,
                    'source':'data_news', 'marker':'manifest.json', 'private_only':True,
                    'preserve_c_original_until_sealed':True}
            body['identity_sha256'] = hashlib.sha256(json.dumps(body, sort_keys=True, separators=(',', ':')).encode()).hexdigest()
            (handoff.CONTROL / 'news-enrollment.json').write_text(json.dumps(body))
        if sys.argv[3] == 'data_news_fulltext/bodies':
            handoff.CONTROL.mkdir()
            body = {'contract':'stockagent.news_bodies_source_handoff_enrollment.v1',
                    'name':handoff.NEWS_BODIES_NAME, 'source':'data_news_fulltext/bodies',
                    'marker':handoff.NEWS_BODIES_MARKER, 'private_only':True,
                    'preserve_c_original_until_sealed':True}
            body['identity_sha256'] = hashlib.sha256(json.dumps(body, sort_keys=True, separators=(',', ':')).encode()).hexdigest()
            (handoff.CONTROL / 'news-bodies-enrollment.json').write_text(json.dumps(body))
        if sys.argv[3] == 'data_tw_microstructure/hft_dataset':
            handoff.CONTROL.mkdir()
            body = {'contract':'stockagent.hft_source_handoff_enrollment.v1', 'name':handoff.HFT_NAME,
                    'source':'data_tw_microstructure/hft_dataset', 'marker':'manifest.json',
                    'preserve_c_original_until_sealed':True}
            body['identity_sha256'] = hashlib.sha256(json.dumps(body, sort_keys=True, separators=(',', ':')).encode()).hexdigest()
            (handoff.CONTROL / 'hft-enrollment.json').write_text(json.dumps(body))
        if sys.argv[3] == 'data_tw_minute/research_dataset':
            handoff.CONTROL.mkdir()
            body = {'contract':'stockagent.minute_source_handoff_enrollment.v1',
                    'name':handoff.MINUTE_NAME, 'source':'data_tw_minute/research_dataset',
                    'marker':'manifest.json', 'preserve_c_original_until_sealed':True,
                    'destination_independent_readback':False}
            body['identity_sha256'] = hashlib.sha256(json.dumps(body, sort_keys=True, separators=(',', ':')).encode()).hexdigest()
            (handoff.CONTROL / 'minute-enrollment.json').write_text(json.dumps(body))
        if is_futures:
            handoff.CONTROL.mkdir()
            body = {'contract':'stockagent.futures_source_handoff_enrollment.v1',
                    'name':handoff.FUTURES_NAME, 'source':'data_tw_index_futures/training_prepared',
                    'marker':handoff.FUTURES_MARKER, 'preserve_c_original_until_sealed':True,
                    'preserve_internal_symlinks':True}
            body['identity_sha256'] = hashlib.sha256(json.dumps(body, sort_keys=True, separators=(',', ':')).encode()).hexdigest()
            (handoff.CONTROL / 'futures-enrollment.json').write_text(json.dumps(body))
        if is_725:
            handoff.CONTROL.mkdir()
            body = {'contract':'stockagent.futures_725_source_handoff_enrollment.v1',
                    'name':handoff.FUTURES_725_NAME,
                    'source':'data_tw_index_futures/training_prepared/tw_futures_universe725_20261025_v2',
                    'marker':'requested_725.csv', 'preserve_c_original_until_sealed':True,
                    'exclude_factorized_sibling':True}
            body['identity_sha256'] = hashlib.sha256(json.dumps(body,sort_keys=True,separators=(',', ':')).encode()).hexdigest()
            (handoff.CONTROL/'futures-725-enrollment.json').write_text(json.dumps(body))
        if is_765:
            handoff.CONTROL.mkdir()
            body = {'contract':'stockagent.futures_765_source_handoff_enrollment.v1',
                    'name':handoff.FUTURES_765_NAME,
                    'source':'data_tw_index_futures/training_prepared/tw_futures_universe765_20261027_v1',
                    'marker':'financial_all_twd_v8/requested_universe.csv',
                    'preserve_c_original_until_sealed':True, 'preserve_internal_symlink_count':1,
                    'exclude_factorized_sibling':True}
            body['identity_sha256'] = hashlib.sha256(json.dumps(body,sort_keys=True,separators=(',', ':')).encode()).hexdigest()
            (handoff.CONTROL/'futures-765-enrollment.json').write_text(json.dumps(body))
        if sys.argv[3] == 'data_yahoo':
            import contextlib
            from stockagent.data_sync import yahoo_workspace_guard
            handoff.CONTROL.mkdir()
            handoff.YAHOO_WRITER_GUARD_FILES = ()
            (handoff.CONTROL/'yahoo-enrollment.json').write_text(json.dumps(handoff.yahoo_enrollment_value()))
            handoff._yahoo_publication_owner = lambda: contextlib.nullcontext()
            yahoo_workspace_guard.YAHOO_ROOT = source
        if sys.argv[3] == 'service_data/stockagent-live/data_bybit':
            import contextlib
            from stockagent.data_sync import bybit_workspace_guard
            handoff.CONTROL.mkdir()
            handoff.BYBIT_WRITER_GUARD_FILES = ()
            (handoff.CONTROL/'bybit-enrollment.json').write_text(json.dumps(handoff.bybit_enrollment_value()))
            handoff._market_publication_owner = lambda dataset: contextlib.nullcontext()
            bybit_workspace_guard.SOURCE = source
            bybit_workspace_guard.DESTINATION = destination
        if sys.argv[3] == 'data_peperstone':
            import contextlib
            from stockagent.data_sync import yahoo_workspace_guard
            handoff.CONTROL.mkdir()
            handoff.PEPPERSTONE_WRITER_GUARD_FILES = ()
            (handoff.CONTROL/'pepperstone-enrollment.json').write_text(json.dumps(handoff.pepperstone_enrollment_value()))
            handoff._market_publication_owner = lambda dataset: contextlib.nullcontext()
            yahoo_workspace_guard.PEPPERSTONE_ROOT = source
        if sys.argv[3] in {'data_cftc_legacy', 'data_fred_crypto_macro', 'data_taifex_public_history', 'data_tw_index_options_daily/raw'}:
            import contextlib
            handoff.CONTROL.mkdir()
            name = sys.argv[2]
            handoff.RETAINED_LOCK_SOURCES[name] = {**handoff.RETAINED_LOCK_SOURCES[name], 'guard_files':()}
            spec = handoff.RETAINED_LOCK_SOURCES[name]
            (handoff.CONTROL/(spec['label']+'-enrollment.json')).write_text(json.dumps(handoff.retained_enrollment_value(name)))
            if is_taifex or is_options_raw:
                handoff._market_publication_owner = lambda dataset: contextlib.nullcontext()
            if is_options_raw:
                from stockagent.data_sync import tw_option_raw_workspace_guard
                tw_option_raw_workspace_guard.SOURCE = source
                tw_option_raw_workspace_guard.DESTINATION = destination
        handoff.enrolled = lambda: (item, workspace, destination, evidence, report_path)
        queue.paths = lambda _item, _workspace: (source, destination, evidence, report_path)
        queue.verified_report = lambda _item, _workspace: report
        handoff.volume.check = lambda *a, **k: {'state':'mounted'}
        handoff.binance_handoff_window_allowed = lambda **kw: True
        handoff.options_raw_handoff_window_allowed = lambda **kw: True
        if is_725 or is_765:
            import contextlib
            handoff._futures_publication_owner = lambda: contextlib.nullcontext()
        if sys.argv[3] == 'data_binance_archive':
            handoff.EXTRA_SOURCES[sys.argv[2]] = (sys.argv[3], 'binance-public-archive')
            handoff.IN_MEMORY_VERIFICATION_ENTRY_LIMIT = 0
        handoff._owner_clear = lambda item: None
        # tmpfs does not support immutable flags. The production owner retains
        # that separate chattr gate; this test exercises actual mount syscalls.
        handoff.immutable_placeholder = lambda path: None
        def own_namespace(*, with_roots):
            assert with_roots
            ns = os.open('/proc/self/ns/mnt', os.O_RDONLY)
            fd = os.open('/', os.O_RDONLY | os.O_DIRECTORY)
            return {os.fstat(ns).st_ino:(ns,fd)}
        provider.namespaces = own_namespace
        try:
            with handoff.selected_source(item['name']):
                scope = handoff._scope()
                result = handoff.handoff()
                assert result['state'] == 'active' and result['namespaces'] == 1
                assert source.samefile(destination) and (source/'original.bin').read_bytes() == b'original-source-content'
                if is_futures or is_765:
                    assert (source/'internal-link').read_bytes() == b'original-source-content'
                if is_765:
                    assert (source.parent/'factorized-sibling').read_text() == 'unrelated'
                assert (scope.backup/'original.bin').read_bytes() == b'original-source-content'
                assert handoff.guard_path(source)['state'] == 'active'
                if sys.argv[3] in {'data_yahoo', 'data_peperstone'}:
                    with yahoo_workspace_guard.yahoo_writer_guard(source/'original.bin'):
                        assert (source/'original.bin').read_bytes() == b'original-source-content'
                if sys.argv[3] == 'service_data/stockagent-live/data_bybit':
                    with bybit_workspace_guard.writer_guard(repo/'data_bybit/original.bin'):
                        assert (source/'original.bin').read_bytes() == b'original-source-content'
                if sys.argv[3] == 'data_fred_crypto_macro':
                    from stockagent.data_sync.retained_source import collector_owner
                    with collector_owner(source, 'fred-crypto-macro') as fd:
                        assert fd >= 3
                if is_taifex:
                    from stockagent.data_sync import taifex_workspace_guard
                    taifex_workspace_guard.SOURCE = source
                    with taifex_workspace_guard.writer_guard(source/'rules', timeout_seconds=0):
                        assert (source/'original.bin').read_bytes() == b'original-source-content'
                if is_options_raw:
                    with tw_option_raw_workspace_guard.writer_guard(source/'original.bin'):
                        assert (source/'original.bin').read_bytes() == b'original-source-content'
                    assert (source.parent/'txo_final_settlement_history.parquet').read_bytes() == b'live-settlement'
                if sys.argv[3] in {'data_binance', 'data_okx'}:
                    assert handoff.guard_path(source/'1m')['state'] == 'active'
                subprocess.run(['umount', str(source)], check=True)
                try:
                    handoff.guard_path(source)
                    raise AssertionError('missing mount allowed a write')
                except SnapshotError:
                    pass
                if sys.argv[3] == 'service_data/stockagent-live/data_bybit':
                    try:
                        with bybit_workspace_guard.writer_guard(repo/'data_bybit/1m'):
                            raise AssertionError('missing Bybit E mount allowed a write')
                    except SnapshotError:
                        pass
                if is_taifex:
                    try:
                        with taifex_workspace_guard.writer_guard(source/'rules', timeout_seconds=0):
                            raise AssertionError('missing TAIFEX E mount allowed a write')
                    except SnapshotError:
                        pass
                if is_options_raw:
                    try:
                        with tw_option_raw_workspace_guard.writer_guard(source/'annual'):
                            raise AssertionError('missing option E mount allowed a write')
                    except SnapshotError:
                        pass
                if sys.argv[3] in {'data_binance', 'data_okx'}:
                    try:
                        handoff.guard_path(source/'1m')
                        raise AssertionError('missing Binance mount allowed a nested writer')
                    except SnapshotError:
                        pass
                resumed = handoff.handoff(boot=True)
                assert resumed['state'] == 'active' and source.samefile(destination)
                assert (scope.backup/'original.bin').read_bytes() == b'original-source-content'
                print('kernel_mount_boot_restore_verified', flush=True)
        finally:
            if os.path.ismount(source):
                subprocess.run(['umount', str(source)], check=True)
    ''')
    env = {**os.environ, 'PYTHONPATH': str(root)}
    run = subprocess.run(['unshare', '--mount', '--propagation', 'private', sys.executable,
                          '-c', script, str(tmp_path), source_name, root_name],
                         env=env, text=True, capture_output=True, timeout=45)
    assert run.returncode == 0, run.stderr
    assert 'kernel_mount_boot_restore_verified' in run.stdout
