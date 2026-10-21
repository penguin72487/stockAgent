"""Representative receipt measurements keep full inputs and original replay."""
import hashlib
import json

import pytest

from downloader import news_fulltext_queue as queue_module
from downloader.artifact_io import sha256_file
from scripts.benchmark_news_acceptance_recovery import benchmark, main
from test_news_acceptance_recovery import accepted_queue, config
from test_news_receipt_periodic_reprobe import committed_inputs


@pytest.mark.parametrize('total,maximum', [(17,5),(5,17),(1,1),(11,1),(12,6)])
def test_stratified_one_snapshot_selection_includes_history_endpoints_and_exact_inputs(config,tmp_path,monkeypatch,total,maximum):
    queue,http=accepted_queue(config,tmp_path)
    try:
        committed_inputs(queue,total)
        pairs=queue.db.execute('SELECT path,receipt_path FROM accepted_batches ORDER BY path').fetchall()
        size=min(total,maximum)
        positions=({i*(total-1)//(size-1) for i in range(size)} if size>1 else {total//2})
        selected=sorted((tuple(pair) for i,pair in enumerate(pairs) if i in positions),key=lambda pair:pair[1])
        expected=hashlib.sha256(json.dumps([[*pair,sha256_file(tmp_path/pair[1])] for pair in selected],sort_keys=True).encode()).hexdigest()
        decoded=[]
        original=queue_module.decode_private_json
        def decode(value):
            result=original(value);decoded.append(result['parquet_path']);return result
        monkeypatch.setattr(queue_module,'decode_private_json',decode)
        changes=queue.db.total_changes
        report=benchmark(tmp_path,maximum_receipts=maximum,cycles=1,cohort_sampling='stratified',
            batched_read_ahead_workers=2,reference_batched_read_ahead_workers=1)
        assert report['accepted'] and report['contract_version']==9
        assert report['cohort_receipts']==size and report['cohort_sha256']==expected
        assert report['source_committed_receipts_at_selection']==total
        assert report['cohort_sampling']=='stratified' and report['cohort_selection_contract_version']==1
        assert report['decode_parity']['all_fields_equal'] and report['source_receipts_unchanged']
        assert decoded==[pair[0] for pair in selected]*4
        assert queue.db.total_changes==changes and not report['production_database_written']
        assert report['publisher_requests']==0 and not report['all_history_complete']
        assert [r['mode'] for r in report['results']]==['binary_batched_prefetch_1','binary_batched_prefetch_2',
            'binary_batched_prefetch_2','binary_batched_prefetch_1']
        assert report['reference_batched_read_ahead_workers']==1 and report['candidate_batched_read_ahead_workers']==2
        assert all(r['sqlite_changes']==0 and r['receipt_json_reads']==size
            and r['committed_lookup_statements']==(size+127)//128 for r in report['results'])
    finally:
        queue.close();http.close()


def test_default_keeps_existing_prefix_identity_and_contract(config,tmp_path):
    queue,http=accepted_queue(config,tmp_path)
    try:
        committed_inputs(queue,11)
        pairs=[tuple(r) for r in queue.db.execute('SELECT path,receipt_path FROM accepted_batches ORDER BY path LIMIT 5')]
        pairs.sort(key=lambda r:r[1])
        expected=hashlib.sha256(json.dumps([[*pair,sha256_file(tmp_path/pair[1])] for pair in pairs],sort_keys=True).encode()).hexdigest()
        report=benchmark(tmp_path,maximum_receipts=5,cycles=1,batched_read_ahead_workers=2)
        assert report['accepted'] and report['contract_version']==5
        assert report['cohort_sampling']=='prefix' and report['cohort_sha256']==expected
        assert report['source_committed_receipts_at_selection'] is None
        assert report['reference_batched_read_ahead_workers']==0
        assert set(report['medians'])=={'binary_serial','binary_batched_prefetch'}
    finally:
        queue.close();http.close()


@pytest.mark.parametrize('options',[
    {'cohort_sampling':True},{'cohort_sampling':'random'},{'cohort_sampling':None},
    {'reference_batched_read_ahead_workers':True,'batched_read_ahead_workers':2},
    {'reference_batched_read_ahead_workers':-1,'batched_read_ahead_workers':2},
    {'reference_batched_read_ahead_workers':3,'batched_read_ahead_workers':2},
    {'reference_batched_read_ahead_workers':1},
    {'reference_batched_read_ahead_workers':2,'batched_read_ahead_workers':2},
    {'reference_batched_read_ahead_workers':1,'batched_read_ahead_workers':2,'adaptive_read_ahead':True},
])
def test_invalid_sampling_and_reader_axes_rejected_before_source_access(tmp_path,options):
    with pytest.raises(ValueError,match='required'):benchmark(tmp_path,**options)


def test_cli_wires_explicit_stratified_direct_comparison(config,tmp_path):
    queue,http=accepted_queue(config,tmp_path)
    try:
        committed_inputs(queue,9)
        output=tmp_path/'benchmark.json'
        assert main(['--root',str(tmp_path),'--maximum-receipts','3','--cycles','1','--cohort-sampling','stratified',
            '--reference-batched-read-ahead-workers','1','--batched-read-ahead-workers','2','--output',str(output)])==0
        result=json.loads(output.read_bytes())
        assert result['accepted'] and result['contract_version']==9
        assert result['cohort_sampling']=='stratified' and result['source_committed_receipts_at_selection']==9
        assert result['reference_batched_read_ahead_workers']==1 and result['candidate_batched_read_ahead_workers']==2
    finally:
        queue.close();http.close()
