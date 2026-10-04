"""Bounded private futures-gap acquisition using the existing FinMind transport.

This is a gap-list adapter, not a second FinMind scheduler. Account entitlement,
request accounting, global pacing, cooldown, and incremental reserve remain
owned by the canonical FinMind helpers. Download completion is not tape readiness.
"""
from __future__ import annotations

import argparse
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from datetime import date, datetime, UTC
import fcntl
import json
import math
import os
from pathlib import Path
import threading
import time

import polars as pl
import requests

from downloader.artifact_io import atomic_write_json, atomic_write_parquet, sha256_file
from downloader.common import load_env_file
from downloader.download_finmind_complement import SourceError, _fetch_rows
from downloader.finmind_account import verified_account, rate_limiter, backfill_budget
from downloader.finmind_scheduling import fixed_incremental_demand
from stockagent.data.tw_futures_finmind_ticks import SOURCE, SCHEMA_VERSION, source_paths


class AcquisitionStopped(RuntimeError):
    pass


class BoundedSession(requests.Session):
    """Check again after the canonical limiter grants a delayed request slot."""
    def __init__(self, stop_at: datetime | None, stop_file: Path | None):
        super().__init__()
        self.stop_at, self.stop_file = stop_at, stop_file

    def get(self, *args, **kwargs):
        if self.stop_file is not None and self.stop_file.is_file():
            raise AcquisitionStopped('stopped_by_file')
        if self.stop_at is not None and datetime.now(UTC) >= self.stop_at:
            raise AcquisitionStopped('deadline_reached')
        return super().get(*args, **kwargs)


TRANSIENT_ERRORS = frozenset({
    'http_500', 'http_502', 'http_503', 'http_504', 'ReadTimeout',
    'ConnectTimeout', 'ConnectionError', 'ChunkedEncodingError',
})


class DownloadAdmission:
    """Bound concurrency around the existing account-wide limiter and budget."""
    def __init__(self, session, token, traffic_root, *, workers, max_requests,
                 stop_at=None, stop_file=None):
        self.session, self.token, self.root = session, token, traffic_root
        self.workers, self.max_requests = workers, max_requests
        self.stop_at, self.stop_file = stop_at, stop_file
        self.lock = threading.RLock()
        self.stopped = threading.Event()
        self.reason = None
        self.budget = None
        self.error_code = None
        self.requests = 0
        self.limiter = None
        self.limit = None

    def stop(self, reason, error_code=None):
        with self.lock:
            if not self.stopped.is_set():
                self.reason, self.error_code = reason, error_code
                self.stopped.set()

    def _check(self):
        if self.stop_file is not None and self.stop_file.is_file():
            self.stop('stopped_by_file')
        if self.stop_at is not None and datetime.now(UTC) >= self.stop_at:
            self.stop('deadline_reached')
        if self.requests >= self.max_requests:
            self.stop('bounded_batch_complete')
        if self.stopped.is_set():
            raise AcquisitionStopped(self.reason)

    def _account(self):
        try:
            account = verified_account(self.session, self.token, self.root)
        except AcquisitionStopped as exc:
            self.stop(str(exc))
            raise
        except (requests.RequestException, RuntimeError, ValueError) as exc:
            self.stop('pending_account', type(exc).__name__)
            raise AcquisitionStopped(self.reason) from None
        if self.limit != account['official_requests_per_hour']:
            self.limiter = rate_limiter(account)
            self.limit = account['official_requests_per_hour']
        return account

    def wait(self):
        # The same shared pacing key still owns every HTTP attempt, including
        # retries. Budget admission happens AFTER waiting for that shared slot.
        with self.lock:
            self._check()
            self._account()
            limiter = self.limiter
        limiter.wait()
        with self.lock:
            self._check()
            account = self._account()
            now = datetime.now(UTC)
            self.budget = backfill_budget(
                account, self.root,
                fixed_incremental_requests=fixed_incremental_demand(self.root, now),
                in_flight=self.workers + 2, now=now,
            )
            if not self.budget['allowed']:
                self.stop('pending_quota')
                raise AcquisitionStopped(self.reason)
            self.requests += 1

    def defer(self, seconds):
        # Preserve the provider-wide cooldown used by the canonical transport.
        self.limiter.defer(seconds)


def download_one(task, *, root, run, session, admission, token,
                 max_response_bytes, transient_retries, stop_on_source_error):
    product, day = task['product'], task['date']
    data, receipt = source_paths(root, product, day)
    if receipt.is_file():
        previous = run / 'previous_receipts' / product / (str(day) + '.json')
        previous.parent.mkdir(parents=True, exist_ok=True)
        previous.write_bytes(receipt.read_bytes())
    item = dict(source=SOURCE, schema_version=SCHEMA_VERSION, product=product,
                date=str(day), queried_at_utc=datetime.now(UTC).isoformat(),
                query={'dataset':'TaiwanFuturesTick', 'data_id':product, 'start_date':str(day)},
                status='failed', rows=0, training_ready=False, attempts=[])
    for attempt in range(transient_retries + 1):
        started = datetime.now(UTC).isoformat()
        error = None
        try:
            rows = _fetch_rows(session, admission, admission.root, 'TaiwanFuturesTick',
                               token, item['query'], max_response_bytes=max_response_bytes)
            item.update(rows=len(rows), status='complete' if rows else 'source_empty')
            if rows:
                frame = pl.DataFrame(rows, infer_schema_length=None)
                atomic_write_parquet(data, frame)
                item.update(sha256=sha256_file(data), columns=frame.columns)
        except AcquisitionStopped as exc:
            if not item['attempts']:
                return None
            # A transient failure followed by a quota/deadline stop remains
            # pending and is never counted as a completed source.
            item.update(status='retry_pending', stop_reason=str(exc))
            break
        except SourceError as exc:
            error = exc.code
        except (ValueError, TypeError, pl.exceptions.PolarsError) as exc:
            error = 'invalid_response_schema'
            item['error_type'] = type(exc).__name__
        item['attempts'].append(dict(started_at_utc=started,
                                    finished_at_utc=datetime.now(UTC).isoformat(),
                                    error_code=error))
        if error is None:
            item.pop('error_code', None)
            break
        item['error_code'] = error
        if error in TRANSIENT_ERRORS and attempt < transient_retries:
            # This wait holds no global pacing/quota lock. Other work proceeds.
            admission.stopped.wait(min(20., 2. * (2 ** attempt)))
            continue
        break
    item['finished_at_utc'] = datetime.now(UTC).isoformat()
    atomic_write_json(receipt, item)
    if item.get('error_code') in {'invalid_token','not_entitled','rate_limited','ip_banned'}:
        admission.stop('pending_provider', item['error_code'])
    elif stop_on_source_error and item['status'] in {'failed','source_empty'}:
        admission.stop('pending_source_review', item.get('error_code', item['status']))
    return item


def update_download_progress(status, *, remaining, elapsed_seconds):
    """Count durable product-day receipts, not attempts or validated minute days.

    This is O(1) telemetry: no API call or repeated raw-directory scan. Empty
    responses, retries and failures remain pending; tape readiness belongs to
    the downstream official-price/volume/physical-contract validation.
    """
    total = status['unique_product_days']
    if not 0 <= remaining <= total:
        raise ValueError('remaining product days are outside the planned universe')
    completed = status.get('completed', 0)
    rate = completed / elapsed_seconds if completed and elapsed_seconds >= 30 else None
    status.update(
        pending_product_days=remaining,
        downloaded_product_days=total - remaining,
        source_download_percent=100. * (total - remaining) / total if total else 100.,
        source_download_complete=remaining == 0,
        download_progress_unit='product_day',
        completed_product_days_per_minute=rate * 60 if rate is not None else None,
        eta_active_seconds=0 if remaining == 0 else math.ceil(remaining / rate) if rate else None,
        eta_scope='source_download_only_excludes_quota_wait_validation_assembly',
        elapsed_seconds=max(0., elapsed_seconds),
        training_ready=False,
    )


def run_downloads(pending, *, args, run, status, token, stop_at):
    traffic_root = Path('data_finmind')
    sessions = []
    local = threading.local()
    started = time.monotonic()
    status.setdefault('unique_product_days', len(pending) + status.get('already_downloaded', 0))
    status['pending_product_days_at_start'] = len(pending)
    with BoundedSession(stop_at, args.stop_file) as account_session:
        admission = DownloadAdmission(account_session, token, traffic_root,
            workers=args.workers, max_requests=args.max_requests,
            stop_at=stop_at, stop_file=args.stop_file)

        def worker(task):
            if not hasattr(local, 'session'):
                local.session = BoundedSession(stop_at, args.stop_file)
                with admission.lock:
                    sessions.append(local.session)
            return download_one(task, root=args.root, run=run, session=local.session,
                admission=admission, token=token,
                max_response_bytes=args.max_response_mib*1024*1024,
                transient_retries=args.transient_retries,
                stop_on_source_error=args.stop_on_source_error)

        tasks = iter(pending)
        checkpoint_at = 100
        next_checkpoint = started + 30.
        try:
            with ThreadPoolExecutor(max_workers=args.workers) as pool:
                active = set()
                while active or not admission.stopped.is_set():
                    while len(active) < args.workers and not admission.stopped.is_set():
                        task = next(tasks, None)
                        if task is None:
                            break
                        active.add(pool.submit(worker, task))
                    if not active:
                        break
                    done, active = wait(active, return_when=FIRST_COMPLETED,
                                        timeout=max(.05, next_checkpoint - time.monotonic()))
                    for future in done:
                        item = future.result()
                        if item is None:
                            continue
                        counter = {'complete':'completed','source_empty':'empty',
                                   'failed':'failed','retry_pending':'retry_pending'}[item['status']]
                        status[counter] = status.get(counter, 0) + 1
                        status['transient_retries'] += max(0, len(item['attempts']) - 1)
                        status.update(updated_at_utc=item['finished_at_utc'],
                            last_task={k:item[k] for k in ['product','date','status']})
                    now = time.monotonic()
                    with admission.lock:
                        status.update(requests=admission.requests, budget=admission.budget,
                                      official_requests_per_hour=admission.limit)
                    update_download_progress(status, remaining=len(pending) - status['completed'],
                                             elapsed_seconds=now - started)
                    if status['requests'] >= checkpoint_at or now >= next_checkpoint:
                        status.update(state=admission.reason or 'downloading',
                                      progress_observed_at_utc=datetime.now(UTC).isoformat())
                        atomic_write_json(run/'progress.json', status)
                        print(json.dumps(status), flush=True)
                        checkpoint_at = (status['requests'] // 100 + 1) * 100
                        next_checkpoint = now + 30.
        finally:
            for session in sessions:
                session.close()
        status.update(state=admission.reason or 'bounded_batch_complete',
                      requests=admission.requests, budget=admission.budget,
                      official_requests_per_hour=admission.limit)
        update_download_progress(status, remaining=len(pending) - status['completed'],
                                 elapsed_seconds=time.monotonic() - started)
        if admission.error_code:
            status['error_code'] = admission.error_code


def candidate_plan(gaps: Path, *, participation: float,
                   daily_path: Path | None = None,
                   official_evidence: Path | None = None) -> pl.DataFrame:
    if not 0 < participation <= 1:
        raise ValueError('capacity participation must be within (0,1]')
    frame = (pl.read_parquet(gaps) if gaps.suffix == '.parquet'
             else pl.read_csv(gaps,try_parse_dates=True)).with_columns(pl.col('date').cast(pl.Date))
    # A completed raw response can still fail the builder's month/OHLC checks.
    # Its final status owns the unresolved set, never a legacy receipt label.
    final_status = 'status' in frame.columns
    if final_status:
        frame = frame.filter(~pl.col('status').is_in([
            'minute_verified','official_no_outright_trades','official_subcontract_capacity']))
    elif 'raw_source_state' in frame.columns:
        frame = frame.filter(pl.col('raw_source_state') != 'complete_data_exists')
    else:
        raise ValueError('gap table requires final status or legacy raw_source_state')
    missing = [name for name in ['product','asset_class'] if name not in frame.columns]
    if missing:
        if daily_path is None:
            raise ValueError('builder gap identities require --daily-data-path')
        identities = pl.read_parquet(daily_path,columns=['date','physical_contract',*missing])
        frame = frame.join(identities,on=['date','physical_contract'],how='left',validate='1:1')
        if frame.select(pl.any_horizontal(pl.col(c).is_null() for c in missing).any()).item():
            raise ValueError('builder gap identities are absent from the pinned daily source')
    if official_evidence is not None:
        manifest = json.loads(official_evidence.with_name('official_evidence_manifest.json').read_text())
        if sha256_file(official_evidence) != manifest.get('sha256'):
            raise ValueError('official gap evidence SHA mismatch')
        proof = pl.read_parquet(official_evidence,columns=['date','physical_contract','outright_volume'])
        frame = frame.drop([c for c in ['outright_volume','official_outright_class'] if c in frame.columns]).join(
            proof,on=['date','physical_contract'],how='left',validate='1:1')
    if 'outright_volume' not in frame.columns:
        raise ValueError('gap table requires --official-evidence for integer-capacity proof')
    if final_status and frame['outright_volume'].null_count():
        raise ValueError(f"pending official capacity evidence for {frame['outright_volume'].null_count()} builder gaps")
    if 'raw_source_state' not in frame.columns:
        frame = frame.with_columns(pl.col('status').alias('raw_source_state'))
    return frame.filter((pl.col('outright_volume') > 0)
                        & ((pl.col('outright_volume') * participation).floor() >= 1))


def existing_source_candidates(frame: pl.DataFrame, *, daily_path: Path,
                               shioaji_root: Path, repair_root: Path,
                               official_zip_root: Path) -> pl.DataFrame:
    """Defer existing receipts/ZIPs for builder validation; do not bless content."""
    from downloader.download_shioaji_historical_market_data import _valid_receipt
    selected = pl.read_parquet(daily_path, columns=['date','physical_contract','shioaji_roots']).join(
        frame.select('date','physical_contract'), on=['date','physical_contract'], how='semi')
    aliases = {(r['date'],r['physical_contract']):str(r['shioaji_roots'] or '').split(',')
               for r in selected.to_dicts()}
    inventory_path = shioaji_root / 'inventory/contracts.parquet'
    inventory = pl.read_parquet(inventory_path).filter(pl.col('collection') == 'exact_futures') if inventory_path.is_file() else pl.DataFrame()
    by_physical: dict[str,list[dict]] = {}
    if inventory.height:
        for row in inventory.to_dicts():
            by_physical.setdefault(row['root']+':'+row['delivery_month'], []).append(row)
    repairs: dict[str,list[dict]] = {}
    for name in ['repair_plan.json','repair_tick_plan.json']:
        p = repair_root / name
        if p.is_file():
            for row in json.loads(p.read_text()).get('tasks',[]):
                repairs.setdefault(row['physical_contract'],[]).append(row)
    chunk_cache = {}
    result = []
    for row in frame.to_dicts():
        day, physical = row['date'], row['physical_contract']
        found = []
        archive = official_zip_root / ('Daily_' + str(day).replace('-','_') + '.zip')
        if archive.is_file():
            found.append(dict(kind='official_zip_candidate_not_validated', path=str(archive)))
        for task in repairs.get(physical,[]):
            if not task['start'] <= str(day) <= task['end']:
                continue
            method = task['method']
            partition = f"start={task['start']}_end={task['end']}" if method == 'kbars' else f"trading_date={task['start']}"
            folder = repair_root/'contracts/futures'/task['code']/method/partition
            receipt = _valid_receipt(folder/'receipt.json',folder/'data.parquet',method=method,code=task['code'])
            if receipt and receipt['status'] == 'complete':
                found.append(dict(kind='exact_repair_receipt_candidate',path=str(folder/'receipt.json')))
        roots = {physical.split(':')[0],*aliases.get((day,physical),[])} - {''}
        for root in roots:
            for contract in by_physical.get(root+':'+physical.split(':')[1],[]):
                if not contract['begin_date'] <= day <= contract['end_date']:
                    continue
                code = contract['code']
                if code not in chunk_cache:
                    chunks = []
                    base = shioaji_root/'contracts'/contract['asset_class']/code/'kbars'
                    for p in base.glob('*/receipt.json'):
                        saved = _valid_receipt(p,p.with_name('data.parquet'),method='kbars',code=code)
                        if saved and saved['status'] == 'complete':
                            chunks.append((p,saved))
                    chunk_cache[code] = chunks
                for p,saved in chunk_cache[code]:
                    if str(day) in saved.get('observed_trading_dates',[]):
                        found.append(dict(kind='exact_inventory_kbar_receipt_candidate',path=str(p)))
        result.append(dict(date=day,physical_contract=physical,existing_source_candidates=json.dumps(found),deferred_existing=bool(found)))
    return pl.DataFrame(result)


def valid_saved(data: Path, receipt: Path, *, product: str, day: date) -> bool:
    try:
        row = json.loads(receipt.read_text())
        return (row.get('source') == SOURCE and row.get('schema_version') == SCHEMA_VERSION
                and row.get('product') == product and row.get('date') == str(day)
                and row.get('status') == 'complete' and sha256_file(data) == row.get('sha256'))
    except (OSError, ValueError, TypeError):
        return False


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--gaps',type=Path,required=True)
    parser.add_argument('--root',type=Path,default=Path('data_finmind/futures_intraday'))
    parser.add_argument('--max-requests',type=int,default=3000)
    parser.add_argument('--capacity-participation',type=float,default=.5)
    parser.add_argument('--daily-data-path',type=Path,default=Path('artifacts/data_preparation/futures_daily_70a57dd76de74fd0a365/continuous_daily.parquet'))
    parser.add_argument('--official-evidence',type=Path,help='SHA-bound official proof covering the final builder gaps.')
    parser.add_argument('--max-response-mib',type=int,default=32,help='Bound decoded response bytes; use a measured larger bound for high-volume product days.')
    parser.add_argument('--workers',type=int,default=1,help='Concurrent HTTP/parse workers sharing the existing account-wide limiter.')
    parser.add_argument('--transient-retries',type=int,default=2,help='Bounded retries for temporary transport/5xx errors; each retry consumes shared quota.')
    parser.add_argument('--include-deferred',action='store_true',help='Reconsider existing-source candidates after the builder has rejected them.')
    parser.add_argument('--plan-only',action='store_true')
    parser.add_argument('--stop-at-utc',help='Stop before dispatching another request at this timezone-aware ISO timestamp.')
    parser.add_argument('--stop-file',type=Path,help='Stop before dispatching another request when this local file exists.')
    parser.add_argument('--stop-on-source-error',action='store_true',help='Preserve the first failed or empty source receipt and stop for review.')
    args = parser.parse_args()
    if not 1 <= args.max_requests <= 3000:
        parser.error('--max-requests must be within 1..3000 for bounded batches')
    if not 1 <= args.max_response_mib <= 512:
        parser.error('--max-response-mib must be within 1..512')
    if not 1 <= args.workers <= 4:
        parser.error('--workers must be within 1..4')
    if not 0 <= args.transient_retries <= 3:
        parser.error('--transient-retries must be within 0..3')
    stop_at = None
    if args.stop_at_utc:
        try:
            stop_at = datetime.fromisoformat(args.stop_at_utc.replace('Z','+00:00'))
            if stop_at.tzinfo is None:
                raise ValueError('timezone required')
            stop_at = stop_at.astimezone(UTC)
        except ValueError:
            parser.error('--stop-at-utc requires a timezone-aware ISO timestamp')
    args.root.mkdir(parents=True,exist_ok=True)
    stamp = datetime.now(UTC).strftime('%Y%m%dT%H%M%S%fZ')
    run = args.root/'runs'/stamp
    run.mkdir(parents=True)
    with (args.root/'download.lock').open('a') as lock:
        fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        plan = candidate_plan(args.gaps,participation=args.capacity_participation,
                              daily_path=args.daily_data_path,official_evidence=args.official_evidence)
        existing = existing_source_candidates(plan,daily_path=args.daily_data_path,
                    shioaji_root=Path('data_tw_shioaji_history'),repair_root=Path('data_tw_futures/shioaji_gap_repair'),
                    official_zip_root=Path('data_tw_index_derivatives_ticks/raw/futures'))
        plan = plan.join(existing,on=['date','physical_contract'],how='left',validate='1:1')
        plan.write_csv(run/'plan.csv')
        plan.filter(pl.col('deferred_existing')).write_csv(run/'deferred_existing.csv')
        eligible = plan if args.include_deferred else plan.filter(~pl.col('deferred_existing'))
        tasks = eligible.select('date','product').unique().sort('date','product').to_dicts()
        pending = [t for t in tasks if not valid_saved(*source_paths(args.root,t['product'],t['date']),product=t['product'],day=t['date'])]
        pl.DataFrame(pending,schema={'date':pl.Date,'product':pl.String}).write_csv(run/'pending.csv')
        status = dict(run_root=str(run),started_at_utc=datetime.now(UTC).isoformat(),
                      state='planned',training_ready=False,gaps_sha256=sha256_file(args.gaps),
                      candidates=plan.height,deferred_candidates=int(plan['deferred_existing'].sum()),
                      unique_product_days=len(tasks),already_downloaded=len(tasks)-len(pending),
                      pending_product_days=len(pending),max_requests=args.max_requests,requests=0,
                      completed=0,empty=0,failed=0,capacity_participation=args.capacity_participation,
                      workers=args.workers,transient_retries=0,
                      transient_retries_per_task=args.transient_retries,
                      max_response_bytes=args.max_response_mib*1024*1024,
                      stop_at_utc=stop_at.isoformat() if stop_at else None,
                      stop_file=str(args.stop_file) if args.stop_file else None,
                      data_rights='private_account_research_only_not_published_or_mirrored')
        update_download_progress(status, remaining=len(pending), elapsed_seconds=0.)
        atomic_write_json(run/'progress.json',status)
        print(json.dumps(status),flush=True)
        if args.plan_only:
            return 0
        load_env_file(Path('.env'),allowed_names=('FINMIND_TOKEN',))
        token = os.environ.get('FINMIND_TOKEN','').strip()
        run_downloads(pending,args=args,run=run,status=status,token=token,stop_at=stop_at)
        remaining = [t for t in pending if not valid_saved(*source_paths(args.root,t['product'],t['date']),product=t['product'],day=t['date'])]
        pl.DataFrame(remaining,schema={'date':pl.Date,'product':pl.String}).write_csv(run/'pending.csv')
        status.update(pending_product_days=len(remaining),finished_at_utc=datetime.now(UTC).isoformat())
        # The final SHA recheck owns the terminal count, including any source
        # that changed externally after its atomic receipt was written.
        update_download_progress(status, remaining=len(remaining),
                                 elapsed_seconds=status['elapsed_seconds'])
        atomic_write_json(run/'progress.json',status)
        print(json.dumps(status),flush=True)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
