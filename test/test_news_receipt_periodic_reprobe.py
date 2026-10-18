"""I/O mode changes never skip inputs, move commits, or trust stale waits."""
from collections import Counter
import json
from pathlib import Path
import threading

import pytest

from downloader import news_fulltext_queue as q
from downloader.artifact_io import atomic_write_json
from test_news_acceptance_recovery import accepted_queue, config


def committed_inputs(queue, total):
    for index in range(total - 1):
        relative = f'bodies/probe-{index:05}.parquet'
        receipt = f'receipts/zz-{index:05}.json'
        atomic_write_json(queue.root / receipt, {'parquet_path':relative, 'literal':-123.75, 'extra':None})
        queue.db.execute('INSERT INTO accepted_batches VALUES (?,?)',(relative,receipt))
    queue.db.commit()
    return sorted((queue.root / 'receipts').glob('*.json'))


def test_cold_warm_cold_reprobes_drain_readers_and_read_each_original_once(config, tmp_path, monkeypatch):
    queue, http = accepted_queue(config,tmp_path)
    try:
        paths = committed_inputs(queue,1027)
        expected = [json.loads(path.read_bytes())['parquet_path'] for path in paths]
        monkeypatch.setattr(q,'RECEIPT_PROBE_STRIDE',2)
        monkeypatch.setattr(q,'RECEIPT_PREFETCH_MAX_CHUNKS',2)
        decisions = iter((2,0,2,0))
        probes, opened, decoded, statements = [], [], [], []
        owner = threading.get_ident()
        original_open, decode = Path.open, q.decode_private_json
        def choose(probe):
            assert not any(t.name.startswith('news-receipt-batch') for t in threading.enumerate())
            probes.append((queue.acceptance_receipt_io['receipts_decoded'],dict(probe)))
            return next(decisions)
        def record_open(path,*args,**kwargs):
            if path.parent == tmp_path/'receipts':
                opened.append(path)
            return original_open(path,*args,**kwargs)
        def record_decode(raw):
            assert threading.get_ident()==owner
            value=decode(raw)
            decoded.append(value['parquet_path'])
            return value
        monkeypatch.setattr(q,'adaptive_receipt_readers',choose)
        monkeypatch.setattr(Path,'open',record_open)
        monkeypatch.setattr(q,'decode_private_json',record_decode)
        queue.db.set_trace_callback(statements.append)
        before=queue.db.total_changes
        queue.recover_acceptance()
        observation=queue.acceptance_receipt_io
        assert observation['adaptive_strategy_version']==2
        assert [position for position,_ in probes]==[128,512,768,1027]
        assert [probe['receipts'] for _,probe in probes]==[128,128,128,3]
        assert Counter(opened)==Counter(paths) and decoded==expected
        assert observation['receipts_decoded']==1027
        assert observation['prefetched_inputs']==512 and observation['synchronous_inputs']==515
        assert observation['prefetch_slices']==2 and observation['reader_mode_switches']==4
        assert observation['started_readers_after_receipts']==128 and observation['chosen_readers']==0
        assert observation['completed'] and queue._acceptance_read_probe is None
        assert queue.db.total_changes==before
        assert sum(text.startswith('SELECT receipt_path,path') for text in statements)==9
        assert not any(text.startswith('SELECT 1 FROM accepted_batches') for text in statements)
        assert not any(t.name.startswith('news-receipt-batch') for t in threading.enumerate())
    finally:
        queue.db.set_trace_callback(None)
        queue.close()
        http.close()


def test_a_persistently_cold_tail_stays_parallel_but_is_reprobed(config,tmp_path,monkeypatch):
    queue,http=accepted_queue(config,tmp_path)
    try:
        committed_inputs(queue,772)
        monkeypatch.setattr(q,'RECEIPT_PREFETCH_MAX_CHUNKS',2)
        positions=[]
        def choose(probe):
            assert not any(t.name.startswith('news-receipt-batch') for t in threading.enumerate())
            positions.append(queue.acceptance_receipt_io['receipts_decoded'])
            return 2 if probe['receipts']==128 else 0
        monkeypatch.setattr(q,'adaptive_receipt_readers',choose)
        queue.recover_acceptance()
        assert positions==[128,512,772]
        observation=queue.acceptance_receipt_io
        assert observation['receipts_decoded']==772
        assert observation['prefetched_inputs']==512 and observation['synchronous_inputs']==260
        assert observation['prefetch_slices']==2 and observation['reader_mode_switches']==2
    finally:
        queue.close()
        http.close()


@pytest.mark.parametrize('readers',[0,2])
def test_explicit_benchmark_readers_keep_their_original_behavior(config,tmp_path,monkeypatch,readers):
    queue,http=accepted_queue(config,tmp_path)
    try:
        committed_inputs(queue,270)
        def reject(_probe):
            raise AssertionError('explicit readers must not invoke adaptive policy')
        monkeypatch.setattr(q,'adaptive_receipt_readers',reject)
        queue.recover_acceptance(read_ahead_workers=readers)
        observation=queue.acceptance_receipt_io
        assert observation['mode']=='explicit' and observation['chosen_readers']==readers
        assert observation['probes_checked']==observation['prefetch_slices']==observation['reader_mode_switches']==0
        assert observation['receipts_decoded']==270 and observation['completed']
    finally:
        queue.close()
        http.close()


@pytest.mark.parametrize('corruption',['directory','json','digest','rows','contract'])
def test_reprobe_error_preserves_earlier_orphan_commit_and_joins_readers(config,tmp_path,monkeypatch,corruption):
    queue,http=accepted_queue(config,tmp_path)
    try:
        committed_inputs(queue,384)
        if corruption in {'directory','json'}:
            with queue.db:
                queue.db.execute('DELETE FROM accepted_batches WHERE path IN (SELECT parquet_path FROM articles)')
            path=tmp_path/'receipts'/'zzzz-invalid.json'
            if corruption=='directory': path.mkdir()
            else: path.write_text('{invalid',encoding='utf-8')
        else:
            path=next(p for p in (tmp_path/'receipts').glob('*.json') if not p.name.startswith('zz-'))
            value=json.loads(path.read_bytes())
            if corruption=='digest': value['sha256']='0'*64
            elif corruption=='rows': value['rows']+=1
            else: value['contract_version']=-1
            atomic_write_json(path,value)
            with queue.db:
                queue.db.execute('DELETE FROM accepted_batches WHERE path IN (SELECT parquet_path FROM articles)')
        monkeypatch.setattr(q,'RECEIPT_PREFETCH_MAX_CHUNKS',2)
        monkeypatch.setattr(q,'adaptive_receipt_readers',lambda _probe:2)
        with pytest.raises(IsADirectoryError if corruption=='directory' else ValueError):
            queue.recover_acceptance()
        assert queue._acceptance_read_probe is None
        assert not queue.acceptance_receipt_io['completed']
        assert not any(t.name.startswith('news-receipt-batch') for t in threading.enumerate())
        assert queue.db.execute('SELECT count(*) FROM accepted_batches').fetchone()[0]==(384 if corruption in {'directory','json'} else 383)
    finally:
        queue.close()
        http.close()


def test_reprobe_benchmark_compares_sticky_mode_with_periodic_warm_tail(config,tmp_path,monkeypatch):
    from scripts.benchmark_news_acceptance_recovery import benchmark
    queue,http=accepted_queue(config,tmp_path)
    try:
        committed_inputs(queue,1027)
        monkeypatch.setattr(q,'RECEIPT_PREFETCH_MAX_CHUNKS',2)
        monkeypatch.setattr(q,'adaptive_receipt_readers',lambda _probe:0)
        original_choose,original_limit=q.adaptive_receipt_readers,q.RECEIPT_PREFETCH_MAX_CHUNKS
        report=benchmark(tmp_path,maximum_receipts=1027,cycles=1,
            adaptive_reprobe_only=True,seed_wait_bound_prefix=True)
        assert report['accepted'] and report['contract_version']==8
        assert report['wait_bound_initial_admission_seeded_equally']
        assert report['initial_admission_seed_is_not_a_cold_boot_measurement']
        assert report['decode_parity']['all_fields_equal'] and report['source_receipts_unchanged']
        assert not report['production_database_written'] and report['publisher_requests']==0
        assert q.adaptive_receipt_readers is original_choose and q.RECEIPT_PREFETCH_MAX_CHUNKS==original_limit
        for row in report['results']:
            assert row['sqlite_changes']==0 and row['receipt_io']['receipts_decoded']==1027
            assert row['committed_lookup_statements']==9
            io=row['receipt_io']
            if row['mode']=='sticky_batched_prefetch':
                assert io['chosen_readers']==2 and io['probes_checked']==1
                assert io['prefetched_inputs']==899
            else:
                assert row['mode']=='periodic_batched_prefetch'
                assert io['chosen_readers']==0 and io['probes_checked']>1
                assert io['prefetched_inputs']==256
    finally:
        queue.close()
        http.close()


@pytest.mark.parametrize('options',[
    {'seed_wait_bound_prefix':True}, {'adaptive_reprobe_only':1},
    {'adaptive_reprobe_only':True,'seed_wait_bound_prefix':1},
    {'adaptive_reprobe_only':True,'adaptive_read_ahead':True},
    {'adaptive_reprobe_only':True,'path_identity_only':True},
])
def test_reprobe_benchmark_requires_a_separate_explicit_comparison(tmp_path,options):
    from scripts.benchmark_news_acceptance_recovery import benchmark
    with pytest.raises(ValueError,match='comparisons?_required'):
        benchmark(tmp_path,**options)
