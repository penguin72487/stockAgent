from datetime import UTC, date, datetime, timedelta
import json
from pathlib import Path
import threading
import time
from types import SimpleNamespace

import pytest

from downloader.download_finmind_complement import SourceError
from scripts import download_tw_futures_intraday_gaps as subject


def setup_admission(monkeypatch, tmp_path, *, maximum=20, workers=4, stop_file=None):
    class Limiter:
        def wait(self):
            pass
        def defer(self, seconds):
            self.deferred = seconds
    limiter = Limiter()
    monkeypatch.setattr(subject, 'verified_account', lambda *a: {'official_requests_per_hour':6000})
    monkeypatch.setattr(subject, 'rate_limiter', lambda account: limiter)
    monkeypatch.setattr(subject, 'fixed_incremental_demand', lambda *a: 479)
    monkeypatch.setattr(subject, 'backfill_budget', lambda *a, **kw: {'allowed':True,'reserve':481})
    admission = subject.DownloadAdmission(None, 'test-token', tmp_path, workers=workers,
        max_requests=maximum, stop_file=stop_file)
    admission.stopped.wait = lambda seconds: admission.stopped.is_set()
    return admission, limiter


def execute_one(tmp_path, admission, **kwargs):
    run = tmp_path/'run'
    run.mkdir(exist_ok=True)
    return subject.download_one({'product':'TJF','date':date(2022,5,26)},
        root=tmp_path/'raw', run=run, session=None, admission=admission,
        token='test-token', max_response_bytes=1024, transient_retries=2,
        stop_on_source_error=True, **kwargs)


def test_transient_retry_uses_shared_admission_and_preserves_attempt_evidence(monkeypatch,tmp_path):
    admission, _ = setup_admission(monkeypatch,tmp_path)
    def fetch(session, limiter, *args, **kwargs):
        limiter.wait()
        if limiter.requests == 1:
            raise SourceError('http_502')
        return [{'price':10.,'volume':2}]
    monkeypatch.setattr(subject,'_fetch_rows',fetch)
    item = execute_one(tmp_path,admission)
    assert item['status']=='complete' and admission.requests==2
    assert [a['error_code'] for a in item['attempts']]==['http_502',None]
    assert not admission.stopped.is_set()


@pytest.mark.parametrize('error',['invalid_token','not_entitled','rate_limited','invalid_response_schema'])
def test_permanent_errors_are_not_retried(monkeypatch,tmp_path,error):
    admission, _ = setup_admission(monkeypatch,tmp_path)
    def fetch(session, limiter, *args, **kwargs):
        limiter.wait()
        raise SourceError(error)
    monkeypatch.setattr(subject,'_fetch_rows',fetch)
    item = execute_one(tmp_path,admission)
    assert item['status']=='failed' and len(item['attempts'])==1
    assert admission.requests==1 and admission.stopped.is_set()


def test_quota_rechecked_after_global_wait_before_any_http(monkeypatch,tmp_path):
    admission, limiter = setup_admission(monkeypatch,tmp_path)
    def shared_wait():
        monkeypatch.setattr(subject,'backfill_budget',lambda *a,**kw:{'allowed':False,'reserve':481})
    limiter.wait = shared_wait
    with pytest.raises(subject.AcquisitionStopped,match='pending_quota'):
        admission.wait()
    assert admission.requests==0


def test_stop_file_created_during_global_wait_blocks_dispatch(monkeypatch,tmp_path):
    stop=tmp_path/'STOP'
    admission, limiter=setup_admission(monkeypatch,tmp_path,stop_file=stop)
    limiter.wait=stop.touch
    with pytest.raises(subject.AcquisitionStopped,match='stopped_by_file'):
        admission.wait()
    assert admission.requests==0


def test_request_cap_includes_retries_and_retains_pending_failure(monkeypatch,tmp_path):
    admission, _=setup_admission(monkeypatch,tmp_path,maximum=2)
    def fetch(session,limiter,*a,**kw):
        limiter.wait()
        raise SourceError('http_504')
    monkeypatch.setattr(subject,'_fetch_rows',fetch)
    item=execute_one(tmp_path,admission)
    assert admission.requests==2
    assert item['status']=='retry_pending'
    assert len(item['attempts'])==2
    assert admission.reason=='bounded_batch_complete'


def test_parallel_pool_caps_all_attempts_without_losing_completed_receipts(monkeypatch,tmp_path):
    setup_admission(monkeypatch,tmp_path)
    active=0
    peak=0
    lock=threading.Lock()
    def fetch(session,limiter,*a,**kw):
        nonlocal active,peak
        limiter.wait()
        with lock:
            active+=1
            peak=max(peak,active)
        time.sleep(.03)
        with lock:
            active-=1
        return [{'price':10.,'volume':2}]
    monkeypatch.setattr(subject,'_fetch_rows',fetch)
    args=SimpleNamespace(root=tmp_path/'raw',workers=4,max_requests=7,
        stop_file=None,max_response_mib=1,transient_retries=2,stop_on_source_error=True)
    run=tmp_path/'run'
    run.mkdir()
    status=dict(requests=0,completed=0,failed=0,empty=0,transient_retries=0)
    pending=[dict(product=f'T{i}F',date=date(2022,5,26)) for i in range(20)]
    subject.run_downloads(pending,args=args,run=run,status=status,token='test-token',stop_at=None)
    assert 2<=peak<=4
    assert status['requests']==status['completed']==7
    assert status['failed']==0 and status['state']=='bounded_batch_complete'
    assert status['pending_product_days'] == 13
    assert status['downloaded_product_days'] == 7
    assert status['source_download_percent'] == 35.
    assert status['official_requests_per_hour'] == 6000
    assert status['budget']['reserve'] == 481
    assert not status['source_download_complete']
    assert not status['training_ready']
    receipts=list(args.root.glob('*/*/*/receipt.json'))
    assert len(receipts)==7
    assert all(json.loads(p.read_text())['status']=='complete' for p in receipts)


def test_deadline_prevents_account_lookup_and_data_dispatch(monkeypatch,tmp_path):
    admission,_=setup_admission(monkeypatch,tmp_path)
    admission.stop_at=datetime.now(UTC)-timedelta(seconds=1)
    monkeypatch.setattr(subject,'verified_account',lambda *a:pytest.fail('No HTTP after deadline'))
    with pytest.raises(subject.AcquisitionStopped,match='deadline_reached'):
        admission.wait()
    assert admission.requests==0


def test_download_progress_counts_durable_product_days_not_retries():
    status = dict(unique_product_days=20, already_downloaded=5, completed=3,
                  requests=6, empty=1, failed=1, retry_pending=1)
    subject.update_download_progress(status, remaining=12, elapsed_seconds=60)
    assert status['downloaded_product_days'] == 8
    assert status['pending_product_days'] == 12
    assert status['source_download_percent'] == 40.
    assert status['completed_product_days_per_minute'] == 3.
    assert status['eta_active_seconds'] == 240
    assert not status['source_download_complete']
    assert not status['training_ready']
    assert 'excludes_quota_wait_validation_assembly' in status['eta_scope']


@pytest.mark.parametrize('elapsed,completed', [(0, 0), (20, 3), (60, 0)])
def test_progress_does_not_invent_eta_without_observed_success(elapsed, completed):
    status = dict(unique_product_days=10, completed=completed)
    subject.update_download_progress(status, remaining=10-completed, elapsed_seconds=elapsed)
    assert status['eta_active_seconds'] is None
    assert status['completed_product_days_per_minute'] is None


@pytest.mark.parametrize('total', [0, 10])
def test_download_complete_does_not_claim_training_ready(total):
    status = dict(unique_product_days=total, completed=0)
    subject.update_download_progress(status, remaining=0, elapsed_seconds=0)
    assert status['source_download_complete']
    assert status['source_download_percent'] == 100.
    assert status['eta_active_seconds'] == 0
    assert not status['training_ready']


def test_progress_can_be_corrected_by_terminal_sha_recheck():
    status = dict(unique_product_days=10, completed=10)
    subject.update_download_progress(status, remaining=0, elapsed_seconds=60)
    subject.update_download_progress(status, remaining=1, elapsed_seconds=60)
    assert status['pending_product_days'] == 1
    assert status['downloaded_product_days'] == 9
    assert not status['source_download_complete']
