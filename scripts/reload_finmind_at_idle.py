"""One-off canonical FinMind reload, only inside a proved drained idle window.

No provider requests, task transitions, source writes or trading-owner actions.
Never interrupt an in-flight batch to adopt a performance change. This is an
explicit operator command, not an installed poller or a second queue owner.
"""
from __future__ import annotations

import argparse
from contextlib import ExitStack, closing, contextmanager
from datetime import UTC, datetime, timedelta
import json
import math
import os
from pathlib import Path
import shutil
import signal
import sqlite3
import statistics
import subprocess
import sys
import time

REPO = Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

from downloader.artifact_io import atomic_write_json
from downloader.finmind_queue_indexes import (STATUS_INDEX, ETA_WORK_INDEX, ETA_RECENT_INDEX,
                                             eta_work_index, eta_recent_index)
from downloader.finmind_runtime import idle_heartbeat
from downloader.finmind_scheduling import protected_stock_opening
from scripts.benchmark_finmind_status import AGGREGATE_SQL, file_sha

UNIT = "stockagent-finmind-complement.service"
PROTECTED = ("stockagent-tw-day-trade-simulation.service", "stockagent-shioaji-taifex-bidask.service")
ROOT = REPO / "data_finmind/complement"
ETA_MEASUREMENTS = frozenset({'finmind_full_eta_queue_indexes_v1', 'finmind_full_eta_queue_indexes_v2'})
STORE_MEASUREMENT = 'finmind_content_addressed_store_fence_v1'
PUBLICATION_MEASUREMENT = 'finmind_bounded_json_publication_pipeline_v1'
PRODUCT_VERIFICATION_ACCEPTANCE = 'finmind_finite_product_query_semantics_v1'
PROVIDER_QUERY_ACCEPTANCE = 'canonical_provider_priority_semantics_v1'


def metadata(path, maximum=4 * 1024**2):
    with path.open("rb") as stream:
        raw = stream.read(maximum + 1)
    if len(raw) > maximum:
        raise ValueError("oversized_local_status")
    value = json.loads(raw)
    if not isinstance(value, dict):
        raise ValueError("invalid_local_metadata_object")
    return value


def identity(unit):
    result = subprocess.run(["systemctl", "show", unit, "-p", "ActiveState", "-p", "InvocationID", "-p", "MainPID"],
                            capture_output=True, text=True, check=True, timeout=5)
    return dict(line.split("=", 1) for line in result.stdout.splitlines() if "=" in line)


def preparation_stack(threads):
    """Only owner preparation outside a live batch; never infer from stale UI."""
    main = next((row for row in threads if row.get('thread_name') == 'MainThread'), None)
    if main is None:
        return False
    frames = main.get('frames', [])
    owner = any(frame.get('name') == 'run_once' and
                str(frame.get('filename', '')).endswith('/downloader/download_finmind_complement.py')
                for frame in frames)
    names = {frame.get('name') for frame in frames}
    prohibited = {'run_batch', '_fetch', '_request_independent', '_request', '_store', '_save_result'}
    return bool(owner and names.intersection({'_populate', 'audit_completed_batch', '_status'})
                and not any(frame.get('name') in prohibited
                            for thread in threads for frame in thread.get('frames', [])))


def _preparation_pid(unit=UNIT, module='downloader.download_finmind_complement'):
    group = subprocess.run(['systemctl', 'show', unit, '-p', 'ControlGroup', '--value'],
                           capture_output=True, text=True, check=True, timeout=5).stdout.strip()
    if not group.startswith('/') or group == '/':
        return None
    pids = (Path('/sys/fs/cgroup') / group.lstrip('/') / 'cgroup.procs').read_text().split()
    for value in pids:
        args = (Path('/proc') / value / 'cmdline').read_bytes().split(b'\0')
        if b'-m' in args and args[args.index(b'-m') + 1] == module.encode():
            return int(value)
    return None


def _stacks(pid):
    result = subprocess.run(['py-spy', 'dump', '--pid', str(pid), '--json', '--nonblocking'],
                            capture_output=True, text=True, check=True, timeout=5)
    # py-spy never requests locals, bodies, tokens or HTTP URLs.
    return json.loads(result.stdout)


def reserve_wait_stack(threads, owner):
    """A live reserve wait may wake at any time; this is usable only frozen."""
    main = next((row for row in threads if row.get('thread_name') == 'MainThread'), None)
    if not main or owner not in {'complement', 'sponsor'}:
        return False
    frames = main.get('frames', [])
    prohibited = {'run_batch', '_fetch', '_request_independent', '_request', '_store', '_save_result'}
    return bool(any(f.get('name') == 'wait_for_next_cycle' and
                    str(f.get('filename', '')).endswith('/downloader/finmind_runtime.py') for f in frames)
                and any(str(f.get('filename', '')).endswith(f'/downloader/download_finmind_{owner}.py')
                        for f in frames)
                and not any(f.get('name') in prohibited for t in threads for f in t.get('frames', [])))


@contextmanager
def reserve_boundary(root, before, owner):
    """Freeze the original reactive waiter and prove zero claims before stop."""
    unit = f'stockagent-finmind-{owner}.service'
    module = f'downloader.download_finmind_{owner}'
    pid = _preparation_pid(unit, module)
    if pid is None or not reserve_wait_stack(_stacks(pid), owner):
        yield None
        return
    frozen = False
    try:
        frozen = True
        subprocess.run(['systemctl', 'freeze', unit], check=True, timeout=5)
        proof = None
        status = metadata(root / 'status.json')
        if (identity(unit) == before and _preparation_pid(unit, module) == pid
                and reserve_wait_stack(_stacks(pid), owner)
                and status.get('state') == 'incremental_reserve'
                and status.get('active_task') is None
                and idle_heartbeat(root, status, datetime.now(UTC)).get('state') == 'waiting'):
            with closing(sqlite3.connect((root / 'queue.sqlite3').resolve().as_uri() + '?mode=ro',
                                         uri=True, timeout=.2)) as connection:
                claims = {'inflight_tasks': connection.execute(
                    "SELECT count(*) FROM tasks WHERE state='inflight'").fetchone()[0]}
                for table in ('finmind_object_prefetch_claims', 'complement_year_batch_claims'):
                    if connection.execute('SELECT 1 FROM sqlite_master WHERE name=?', (table,)).fetchone():
                        claims[table] = connection.execute(f'SELECT count(*) FROM {table}').fetchone()[0]
            if not any(claims.values()):
                proof = {'mode': 'frozen_drained_reserve', 'unit': unit, 'pid': pid, 'claims': claims,
                         'last_task': status.get('last_task'),
                         'cycle_observed_at_utc': status['observed_at_utc']}
        yield proof
    finally:
        if frozen:
            subprocess.run(['systemctl', 'thaw', unit], check=True, timeout=5)


@contextmanager
def preparation_boundary(root, before):
    """Freeze only a passively observed preparation window, then re-prove it.

    Covers every process in this one unit. No restart unless both claim tables
    are empty and a fresh frozen stack has no request/publication in progress.
    Every failure/exception thaws it. No assumption that systemd accepts a
    restart job while frozen (the installed systemd may reject that job).
    """
    pid = _preparation_pid()
    if pid is None or not preparation_stack(_stacks(pid)):
        yield None
        return
    frozen = False
    try:
        frozen = True
        subprocess.run(['systemctl', 'freeze', UNIT], check=True, timeout=5)
        proof = None
        if identity(UNIT) == before and _preparation_pid() == pid and preparation_stack(_stacks(pid)):
            with closing(sqlite3.connect((root / 'queue.sqlite3').resolve().as_uri() + '?mode=ro',
                                         uri=True, timeout=.2)) as conn:
                tables = ('finmind_object_prefetch_claims', 'complement_year_batch_claims')
                claims = {name: conn.execute(f'SELECT count(*) FROM {name}').fetchone()[0] for name in tables}
            if not any(claims.values()):
                status = metadata(root / 'status.json')
                proof = {'mode': 'frozen_drained_preparation', 'pid': pid, 'claims': claims,
                         'last_task': status.get('last_task'),
                         'cycle_observed_at_utc': status.get('observed_at_utc')}
        if proof is None:
            subprocess.run(['systemctl', 'thaw', UNIT], check=True, timeout=5)
            frozen = False
        yield proof
    finally:
        if frozen:
            subprocess.run(['systemctl', 'thaw', UNIT], check=True, timeout=5)


def idle_proof(root, now):
    status, waiting = metadata(root / "status.json"), metadata(root / "worker_status.json", 8192)
    reason = waiting.get("reason")
    # Complement's completed-cycle wait has no wake_if. Only quota-reserve
    # waits are reactive in wait_for_next_cycle(); opening protection is a
    # genuine drained wait, but authorize it only while the canonical calendar
    # gate is still in force. Never create an artificial idle window.
    nonreactive = reason in {"batch_complete", "current_queue"} or (
        reason == "protected_opening" and protected_stock_opening(now)
    )
    if (idle_heartbeat(root, status, now).get("state") != "waiting"
            or not nonreactive
            or status.get("active_task") is not None):
        return None
    deadline = datetime.fromisoformat(waiting["next_check_at_utc"])
    if deadline - now < timedelta(seconds=2):
        return None
    with closing(sqlite3.connect((root / "queue.sqlite3").resolve().as_uri() + "?mode=ro", uri=True, timeout=.2)) as connection:
        claims = {}
        for table in ("finmind_object_prefetch_claims", "complement_year_batch_claims"):
            if connection.execute("SELECT 1 FROM sqlite_master WHERE name=?", (table,)).fetchone():
                claims[table] = connection.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
        claims['inflight_tasks'] = connection.execute("SELECT count(*) FROM tasks WHERE state='inflight'").fetchone()[0]
        if any(claims.values()):
            return None
    return {"cycle_observed_at_utc": status["observed_at_utc"], "reason": status["state"],
            "deadline": deadline.isoformat(), "claims": claims, "last_task": status.get("last_task")}


def prior_files(root, last):
    """Bounded preservation sample, not a full source-completeness claim."""
    if not isinstance(last, dict):
        return []
    with closing(sqlite3.connect((root / "queue.sqlite3").resolve().as_uri() + "?mode=ro", uri=True, timeout=.2)) as connection:
        row = connection.execute("SELECT receipt_path FROM tasks WHERE dataset=? AND data_id=? AND partition=?",
                                 tuple(last.get(key) for key in ("dataset", "data_id", "partition"))).fetchone()
    if not row or not row[0]:
        return []
    receipt = (root / row[0]).resolve()
    if not receipt.is_relative_to(root.resolve()):
        raise ValueError("prior_receipt_outside_owner_root")
    value = metadata(receipt)
    paths = [receipt]
    if value.get("parquet_path"):
        parquet = (root / value["parquet_path"]).resolve()
        if not parquet.is_relative_to(root.resolve()):
            raise ValueError("prior_parquet_outside_owner_root")
        paths.append(parquet)
    return [{"path": str(path), "bytes": path.stat().st_size, "sha256": file_sha(path)}
            for path in paths if path.stat().st_size <= 8 * 1024**2]


def terminate_frozen_preparation(proof):
    """Queue default SIGTERM before thaw: no dispatch gap between proof/stop.

    Only the verified Python owner, not a broad PID/process match. A pidfd pins
    its kernel generation. Reject custom/ignored TERM handlers; the default
    disposition terminates before more user code can issue another request.
    The normal systemd restart remains responsible for starting the new owner.
    """
    if (proof.get('mode') not in {'frozen_drained_preparation', 'frozen_drained_reserve'}
            or any(proof.get('claims', {'unknown': 1}).values())):
        raise ValueError('termination_requires_frozen_drained_preparation')
    unit = proof.get('unit', UNIT)
    if unit not in {UNIT, 'stockagent-finmind-sponsor.service'}:
        raise ValueError('termination_requires_original_owner')
    pid = proof['pid']
    with_pid = _pidfd_open(pid)
    try:
        status = dict(line.split(':', 1) for line in (Path('/proc') / str(pid) / 'status').read_text().splitlines()
                      if ':' in line)
        bit = 1 << (signal.SIGTERM - 1)
        if int(status['SigIgn'], 16) & bit or int(status['SigCgt'], 16) & bit:
            raise ValueError('preparation_owner_has_nondefault_sigterm')
        _pidfd_signal(with_pid, signal.SIGTERM)
    finally:
        os.close(with_pid)
    subprocess.run(['systemctl', 'thaw', unit], check=True, timeout=5)


def _pidfd_call(name, arguments, types):
    # Some packaged CPython builds omit these optional Python bindings while
    # the installed Linux/glibc provides the public pidfd APIs. No raw syscall
    # number or architecture assumption; missing APIs fail closed.
    import ctypes
    function = getattr(ctypes.CDLL(None, use_errno=True), name)
    function.argtypes, function.restype = types, ctypes.c_int
    result = function(*arguments)
    if result < 0:
        raise OSError(ctypes.get_errno(), 'pidfd operation failed')
    return result


def _pidfd_open(pid):
    if hasattr(os, 'pidfd_open'):
        return os.pidfd_open(pid)
    import ctypes
    return _pidfd_call('pidfd_open', (pid, 0), (ctypes.c_int, ctypes.c_uint))


def _pidfd_signal(fd, sig):
    if hasattr(signal, 'pidfd_send_signal'):
        return signal.pidfd_send_signal(fd, sig)
    import ctypes
    return _pidfd_call('pidfd_send_signal', (fd, sig, None, 0),
                       (ctypes.c_int, ctypes.c_int, ctypes.c_void_p, ctypes.c_uint))


def code_matches(accepted):
    hashes = accepted.get("code_sha256", {})
    required = {str(REPO / "downloader/download_finmind_complement.py"),
                str(REPO / "downloader/finmind_queue_indexes.py")}
    if accepted.get('measurement_contract') == PRODUCT_VERIFICATION_ACCEPTANCE:
        required.update(str(REPO / name) for name in (
            'downloader/finmind_product_query_verification.py',
            'test/test_finmind_product_query_verification.py',
            'test/test_reload_finmind_at_idle.py', 'scripts/reload_finmind_at_idle.py'))
    if accepted.get('measurement_contract') == PROVIDER_QUERY_ACCEPTANCE:
        required.update(str(REPO / name) for name in (
            'downloader/provider_priority.py', 'downloader/finmind_product_query_verification.py',
            'downloader/finmind_json_pipeline.py', 'test/test_provider_priority.py',
            'downloader/download_finmind_sponsor.py',
            'test/test_finmind_product_query_verification.py',
            'test/test_reload_finmind_at_idle.py', 'scripts/reload_finmind_at_idle.py'))
    if "whole_publication_wall_improvement_observed" in accepted:
        required.add(str(REPO / "downloader/finmind_status_aggregates.py"))
    if accepted.get('measurement_contract') == 'finmind_live_reservation_readers_local_dispatch_v1':
        required.update(str(REPO / 'downloader' / name) for name in (
            'finmind_scheduling.py', 'finmind_supplemental.py', 'finmind_json_pipeline.py'))
    if accepted.get('measurement_contract') in ETA_MEASUREMENTS:
        required.update(str(REPO / 'downloader' / name) for name in (
            'finmind_eta_work.py', 'finmind_eta_telemetry.py', 'finmind_eta.py'))
        required.add(str(REPO / 'scripts/benchmark_finmind_eta_runtime.py'))
        if accepted.get('measurement_contract') == 'finmind_full_eta_queue_indexes_v2':
            required.add(str(REPO / 'downloader/finmind_runtime.py'))
    if accepted.get('measurement_contract') == STORE_MEASUREMENT:
        required.update(str(REPO / 'downloader' / name) for name in (
            'artifact_io.py', 'finmind_parent_recovery.py', 'finmind_updates.py', 'finmind_json_pipeline.py'))
    if accepted.get('measurement_contract') == PUBLICATION_MEASUREMENT:
        required.update(str(REPO / 'downloader' / name) for name in (
            'artifact_io.py', 'finmind_parent_recovery.py', 'finmind_updates.py',
            'finmind_json_pipeline.py', 'finmind_object_pipeline.py', 'finmind_status_aggregates.py'))
        required.add(str(REPO / 'configs/finmind_json_pipeline.json'))
    return (required.issubset(hashes)
            and all(file_sha(Path(path)) == digest for path, digest in hashes.items()))


def accepted_local_measurement(accepted):
    """Require the local verification appropriate to this exact change."""
    if accepted.get('measurement_contract') in {PRODUCT_VERIFICATION_ACCEPTANCE, PROVIDER_QUERY_ACCEPTANCE}:
        import xml.etree.ElementTree as ET
        provider_query = accepted['measurement_contract'] == PROVIDER_QUERY_ACCEPTANCE
        expected = 'provider_priority_semantics_accepted' if provider_query else 'finite_product_query_semantics_accepted'
        if (accepted.get('state') != expected
                or accepted.get('source_only_no_training_injection') is not True
                or accepted.get('exact_output_parity') is not True
                or any(accepted.get(key) != 0 for key in
                       ('provider_calls', 'production_queue_mutations', 'source_writes'))):
            return False
        try:
            path = Path(accepted['semantic_junit_path'])
            if file_sha(path) != accepted['semantic_junit_sha256']:
                return False
            cases = ET.parse(path).getroot().findall('.//testcase')
            required = {
                'test_exact_verification_shape_is_not_hidden_by_dataset_delegation',
                'test_due_refresh_precedes_the_source_check_and_scope_is_exact',
                'test_source_check_does_not_reopen_terminal_empty_or_running_key',
                'test_registered_verification_retains_cooldown',
                'test_changed_plan_cannot_authorize_a_new_query',
                'test_changed_source_pin_rejects_registration_without_task_mutation',
                'test_finite_source_priority_preempts_due_foreign_history_without_changing_tasks',
                'test_finite_priority_obeys_real_release_quota_and_future_reservations',
                'test_canonical_owner_executes_one_exact_check_before_accepted_renewal',
            }
            if provider_query:
                required.update({
                    'test_finmind_exact_priority_is_idempotent_finite_and_preserves_due_refreshes',
                    'test_finite_read_yields_only_foreign_history_reservations_and_respects_dated_releases',
                    'test_unknown_batch_is_rejected_atomically',
                    'test_request_registry_never_reopens_completed_or_unknown_tasks',
                    'test_api_native_split_retains_finite_priority_and_parent_receipt',
                    'test_sponsor_priority_uses_native_release_and_dependency_predicate',
                })
            passed = {case.get('name', '').split('[', 1)[0] for case in cases}
            return bool(len(cases) >= 15 and required <= passed
                        and all(case.find(tag) is None for case in cases for tag in ('failure','error','skipped')))
        except (OSError, ValueError, KeyError, TypeError, ET.ParseError):
            return False
    if accepted.get('measurement_contract') == PUBLICATION_MEASUREMENT:
        trials = accepted.get('trials', [])
        fingerprint = accepted.get('configuration_sha256')
        if (accepted.get('state') != 'eight_full_pipeline_trials_verified'
                or any(accepted.get(key) is not True for key in
                       ('exact_output_parity', 'outputs_identical', 'source_preserved', 'code_stable'))
                or any(accepted.get(key) != 0 for key in
                       ('provider_calls', 'production_queue_mutations', 'source_writes'))
                or accepted.get('tasks_per_pass') != 48 or accepted.get('passes_per_trial') != 2
                or accepted.get('status_writes_per_trial') != 5
                or type(accepted.get('benchmark_device')) is not int
                or accepted.get('benchmark_device') != accepted.get('target_queue_device')
                or not isinstance(accepted.get('boot_id'), str) or not accepted['boot_id']
                or type(accepted.get('source_rows')) is not int or accepted['source_rows'] <= 0
                or not isinstance(fingerprint, str) or len(fingerprint) != 64
                or [row.get('variant') for row in trials] != ['baseline', 'candidate', 'candidate', 'baseline'] * 2):
            return False
        expected = {'calls': 48, 'accepted_requests': 48, 'nonempty_requests': 48, 'failed_tasks': 0}
        for row in trials:
            if (row.get('passes') != [expected, expected]
                    or any(row.get(key) is not True for key in
                           ('existing_parquet_inodes_preserved', 'full_source_readback', 'queue_writer_thread_verified'))
                    or row.get('same_filesystem_as_target_queue') is not True
                    or row.get('max_response_slots') != 16
                    or type(row.get('max_claimed_responses')) is not int
                    or not 1 <= row['max_claimed_responses'] <= 16
                    or row.get('max_publication_workers') != (2 if row['variant'] == 'candidate' else 1)
                    or not isinstance(row.get('output_sha256'), str) or len(row['output_sha256']) != 64
                    or any(type(row.get(key)) not in (int, float) or not math.isfinite(row[key]) or row[key] <= 0
                           for key in ('wall_seconds', 'cpu_seconds'))):
                return False
        medians = {label: statistics.median(row['wall_seconds'] for row in trials if row['variant'] == label)
                   for label in ('baseline', 'candidate')}
        return (len({row['output_sha256'] for row in trials}) == 1
                and accepted.get('medians') == medians and medians['candidate'] < medians['baseline'])
    if accepted.get('measurement_contract') == STORE_MEASUREMENT:
        trials, records = accepted.get('trials', []), accepted.get('records', {})
        if (accepted.get('state') != 'accepted_bounded_store_replay'
                or accepted.get('scope') != 'canonical_store_first_publish_and_same_value_refresh_with_full_footer_and_sha_readback'
                or accepted.get('exact_output_parity') is not True
                or accepted.get('source_parquet_preserved') is not True
                or accepted.get('code_stable') is not True
                or any(accepted.get(key) != 0 for key in
                       ('provider_calls', 'production_queue_mutations', 'production_source_writes'))
                or records.get('tasks') != 48 or records.get('stores_per_trial') != 96
                or type(records.get('rows_per_payload')) is not int or records['rows_per_payload'] <= 0
                or [row.get('variant') for row in trials] != ['baseline', 'candidate', 'candidate', 'baseline'] * 2):
            return False
        if any(row.get('stores') != 96 or row.get('exact_existing_final_inode_preserved') is not True
               or not isinstance(row.get('output_sha256'), str) or len(row['output_sha256']) != 64
               or any(type(row.get(key)) not in (int, float) or not math.isfinite(row[key]) or row[key] <= 0
                      for key in ('elapsed_seconds', 'cpu_seconds')) for row in trials):
            return False
        if len({row['output_sha256'] for row in trials}) != 1:
            return False
        medians = {variant: {key: statistics.median(row[key] for row in trials if row['variant'] == variant)
                             for key in ('elapsed_seconds', 'cpu_seconds')}
                   for variant in ('baseline', 'candidate')}
        return (accepted.get('medians') == medians
                and medians['candidate']['elapsed_seconds'] < medians['baseline']['elapsed_seconds'])
    if accepted.get('measurement_contract') in ETA_MEASUREMENTS:
        trials, writes = accepted.get('trials', []), accepted.get('claim_release_trials', [])
        with_wake = accepted.get('measurement_contract') == 'finmind_full_eta_queue_indexes_v2'
        if (accepted.get('state') != 'accepted_bounded_replay'
                or accepted.get('scope') != ('full_offline_finmind_eta_and_wake_snapshot' if with_wake else
                                             'full_offline_finmind_eta_snapshot')
                or accepted.get('exact_full_output_parity') is not True
                or accepted.get('read_snapshot_preserved') is not True
                or accepted.get('production_queue_writes') != 0 or accepted.get('provider_calls') != 0
                or [row.get('variant') for row in trials] != ['baseline', 'candidate', 'candidate', 'baseline'] * 2
                or [row.get('variant') for row in writes] != ['baseline', 'candidate', 'candidate', 'baseline']):
            return False
        if with_wake and (any(set(row.get('wake_delays', {})) != {'sponsor', 'complement'} for row in trials)
                          or any(row['wake_delays'] != trials[0]['wake_delays'] for row in trials)):
            return False
        if (any(not row.get('output_sha256') for row in trials)
                or len({row['output_sha256'] for row in trials}) != 1
                or any(type(row.get('peak_rss_bytes')) is not int or row['peak_rss_bytes'] <= 0 for row in trials)
                or any(row.get('tasks') != 32 or row.get('transactions') != 64
                       or row.get('exact_task_rows_restored') is not True for row in writes)):
            return False
        for row in trials + writes:
            keys = ('elapsed_seconds', 'cpu_seconds') if 'elapsed_seconds' in row else ('wall_seconds', 'cpu_seconds')
            if any(type(row.get(key)) not in (int, float) or not math.isfinite(row[key]) or row[key] <= 0 for key in keys):
                return False
        medians = {variant: {key: statistics.median(row[key] for row in trials if row['variant'] == variant)
                             for key in ('elapsed_seconds', 'cpu_seconds', 'peak_rss_bytes')}
                   for variant in ('baseline', 'candidate')}
        setup = accepted.get('queue_index_setup', {})
        return bool(accepted.get('medians') == medians
                    and all(medians['candidate'][key] < medians['baseline'][key]
                            for key in ('elapsed_seconds', 'cpu_seconds'))
                    and set(setup) == {'sponsor', 'complement'}
                    and all(row.get('task_changes') == 0 for row in setup.values()))
    if accepted.get('measurement_contract') == 'finmind_live_reservation_readers_local_dispatch_v1':
        trials = accepted.get('trials', [])
        if (accepted.get('state') != 'measurement_complete'
                or accepted.get('snapshot_preserved') is not True
                or accepted.get('code_stable') is not True
                or accepted.get('exact_output_parity') is not True
                or accepted.get('status_connection') != 'native_StatusConnection_fresh_memo_per_trial'
                or accepted.get('local_dispatch_wall_improvement_observed') is not True
                or accepted.get('production_queue_mutations') != 0 or accepted.get('provider_calls') != 0
                or not 4 <= len(trials) <= 20 or len(trials) % 4
                or [row.get('label') for row in trials] != ['baseline', 'candidate', 'candidate', 'baseline'] * (len(trials)//4)):
            return False
        if any(row.get('admissions') != 120 or row.get('status_writes') != 5
               or not isinstance(row.get('wall_seconds'), (int, float))
               or not math.isfinite(row['wall_seconds']) or row['wall_seconds'] <= 0
               or not row.get('output_sha256') for row in trials):
            return False
        if len({row['output_sha256'] for row in trials}) != 1:
            return False
        medians = {label: statistics.median(row['wall_seconds'] for row in trials if row['label'] == label)
                   for label in ('baseline', 'candidate')}
        return accepted.get('medians_seconds') == medians and medians['candidate'] < medians['baseline']
    if "whole_publication_wall_improvement_observed" not in accepted:
        return accepted.get("status_wall_improvement_observed") is True
    return bool(accepted.get("state") == "measurement_complete"
                and accepted.get("whole_publication_wall_improvement_observed") is True
                and accepted.get("all_task_values_equal") is True
                and accepted.get("source_files_stable") is True
                and accepted.get("resource_limits_stable") is True
                and accepted.get("task_count", 0) > 0
                and accepted.get("rows_published_per_trial", 0) > 0
                and [row.get("label") for row in accepted.get("trials", [])]
                    == ["baseline", "candidate", "candidate", "baseline"])


def eta_projection_readback(connection, now):
    """Prove that the original owner installed both exact physical contracts."""
    if not eta_work_index(connection) or not eta_recent_index(connection):
        return None
    work = [row[3] for row in connection.execute(
        f"EXPLAIN QUERY PLAN SELECT dataset,kind,priority,state,count(*),min(next_attempt_at_utc) "
        f"FROM tasks{eta_work_index(connection)} GROUP BY 1,2,3,4")]
    recent = [row[3] for row in connection.execute(
        f"EXPLAIN QUERY PLAN SELECT state,kind,sum(rows),sum(bytes) FROM tasks{eta_recent_index(connection)} "
        "WHERE last_attempt_at_utc>? AND last_attempt_at_utc<=? GROUP BY state,kind",
        ((now - timedelta(days=7)).isoformat(), now.isoformat()))]
    stats = dict(connection.execute("SELECT idx,stat FROM sqlite_stat1 WHERE idx IN (?,?)",
                                    (ETA_WORK_INDEX, ETA_RECENT_INDEX)))
    if (set(stats) != {ETA_WORK_INDEX, ETA_RECENT_INDEX}
            or not any('COVERING INDEX ' + ETA_WORK_INDEX in row for row in work)
            or not any('SEARCH tasks USING COVERING INDEX ' + ETA_RECENT_INDEX in row for row in recent)):
        return None
    return {'inventory_plan': work, 'recent_outcomes_plan': recent, 'index_statistics': stats}


def _main(argv, resources):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--acceptance", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--wait-seconds", type=int, default=300)
    parser.add_argument("--apply", action="store_true")
    parser.add_argument('--owner', choices=('complement', 'sponsor'), default='complement',
                        help='Original queue owner; Sponsor permits only its existing nonreactive idle window')
    parser.add_argument('--allow-preparation-boundary', action='store_true',
                        help='Also permit a frozen, zero-claim preparation boundary; never interrupts an HTTP batch')
    parser.add_argument('--allow-reserve-boundary', action='store_true',
                        help='Also permit an exactly frozen, zero-claim incremental-reserve waiter')
    args = parser.parse_args(argv)
    root = ROOT if args.owner == 'complement' else REPO / 'data_finmind/sponsor'
    unit = UNIT if args.owner == 'complement' else 'stockagent-finmind-sponsor.service'
    if args.owner == 'sponsor' and args.allow_preparation_boundary:
        parser.error('Sponsor reload requires its natural, drained idle window')
    if args.output.exists() or not 0 <= args.wait_seconds <= 600:
        parser.error("new output required; bounded wait must be 0..600 seconds")
    accepted = metadata(args.acceptance)
    source_queue = Path(accepted.get('source_queue', ''))
    if args.owner == 'sponsor':
        manifest = accepted.get('source_manifest', {})
        if accepted.get('measurement_contract') == PROVIDER_QUERY_ACCEPTANCE:
            pass  # Exact semantic evidence and code hashes are checked below for this owner root.
        elif (accepted.get('measurement_contract') in ETA_MEASUREMENTS
                and 'data/sponsor/queue.sqlite3' in manifest.get('files', {})):
            source_queue = Path(manifest.get('source', '')) / 'sponsor/queue.sqlite3'
        else:
            raise ValueError('sponsor_queue_requires_its_full_measured_snapshot')
    if (not accepted.get("exact_output_parity") or not accepted.get("code_stable")
            or not accepted_local_measurement(accepted)
            or source_queue.resolve() != (root / "queue.sqlite3").resolve()
            or not code_matches(accepted)):
        raise ValueError("current_code_not_bound_to_accepted_measurement")
    if accepted.get('measurement_contract') == PUBLICATION_MEASUREMENT:
        if (source_queue.stat().st_dev != accepted['benchmark_device']
                or Path('/proc/sys/kernel/random/boot_id').read_text().strip() != accepted['boot_id']):
            raise ValueError('publication_measurement_requires_current_storage_and_boot')
    if shutil.disk_usage(root).free < 26 * 1024**3:
        raise ValueError("queue_index_headroom_guard")
    before = identity(unit)
    if before.get("ActiveState") != "active" or not before.get("InvocationID") or before.get("MainPID") in {None, "0"}:
        raise ValueError("reload_requires_the_current_running_owner")
    protected_units = PROTECTED + ((UNIT,) if args.owner == 'sponsor' else ())
    protected = {name: identity(name) for name in protected_units}
    end = time.monotonic() + args.wait_seconds
    proof = None
    while True:
        try:
            proof = idle_proof(root, datetime.now(UTC))
            if proof is None and args.apply and args.allow_preparation_boundary:
                with ExitStack() as attempt:
                    proof = attempt.enter_context(preparation_boundary(root, before))
                    if proof:
                        resources.enter_context(attempt.pop_all())
            if proof is None and args.apply and args.allow_reserve_boundary:
                with ExitStack() as attempt:
                    proof = attempt.enter_context(reserve_boundary(root, before, args.owner))
                    if proof:
                        resources.enter_context(attempt.pop_all())
        except (OSError, ValueError, KeyError, sqlite3.Error, subprocess.SubprocessError):
            proof = None
        if proof or time.monotonic() >= end or not args.apply:
            break
        time.sleep(.5)
    report = {"schema_version": 1, "observed_at_utc": datetime.now(UTC).isoformat(), "unit": unit,
              "service_before": before, "idle_proof": proof, "apply_requested": args.apply,
              "restart_requested": False, "provider_calls_by_this_command": 0, "source_writes": 0,
              "queue_task_mutations": 0, "protected_services_before": protected}
    if not proof or not args.apply:
        report["state"] = "deferred_no_safe_idle_window" if not proof else "idle_observed_no_action"
        atomic_write_json(args.output, report)
        return 0
    samples = prior_files(root, proof["last_task"])
    report["sampled_prior_files"] = samples
    # The short batch-complete sleep is non-reactive. Check the exact same
    # service generation and remaining deadline immediately before dispatch.
    frozen = proof.get('mode') in {'frozen_drained_preparation', 'frozen_drained_reserve'}
    if (identity(unit) != before or not code_matches(accepted)
            or not frozen and (idle_proof(root, datetime.now(UTC)) != proof
                or datetime.fromisoformat(proof["deadline"]) - datetime.now(UTC) < timedelta(seconds=1))):
        report["state"] = "deferred_generation_or_deadline_changed"
        atomic_write_json(args.output, report)
        return 0
    stamp = datetime.now(UTC)
    report.update(state="restart_requested", restart_requested=True, restart_at_utc=stamp.isoformat())
    atomic_write_json(args.output, report)  # Retain action evidence even if interrupted.
    if frozen:
        terminate_frozen_preparation(proof)
    subprocess.run(["systemctl", "restart", unit], check=True, timeout=30)
    # A measured first-time projection build can exceed the old 90-second
    # status window. Wait for the owner; never build indexes from this reader.
    until = time.monotonic() + (300 if accepted.get('measurement_contract') in ETA_MEASUREMENTS else 90)
    while time.monotonic() < until:
        after = identity(unit)
        try:
            status = metadata(root / "status.json")
            with closing(sqlite3.connect((root / "queue.sqlite3").resolve().as_uri() + "?mode=ro", uri=True, timeout=.2)) as connection:
                plan = [row[3] for row in connection.execute("EXPLAIN QUERY PLAN " + AGGREGATE_SQL)]
                statistics = connection.execute("SELECT stat FROM sqlite_stat1 WHERE idx=?", (STATUS_INDEX,)).fetchone()
                eta_proof = (eta_projection_readback(connection, datetime.now(UTC))
                             if accepted.get('measurement_contract') in ETA_MEASUREMENTS else None)
            if (after["ActiveState"] == "active" and after["InvocationID"] != before["InvocationID"]
                    and datetime.fromisoformat(status["observed_at_utc"]) > stamp
                    and statistics and any("COVERING INDEX " + STATUS_INDEX in row for row in plan)
                    and (accepted.get('measurement_contract') not in ETA_MEASUREMENTS or eta_proof)):
                report.update(state="canonical_owner_schema_and_status_verified", service_after=after,
                              status_observed_at_utc=status["observed_at_utc"], index_stat=statistics[0], aggregate_plan=plan)
                if eta_proof:
                    report['eta_index_readback'] = eta_proof
                break
        except (OSError, ValueError, KeyError, sqlite3.Error):
            pass
        time.sleep(1)
    else:
        report["state"] = "restart_readback_unverified"
    report["protected_services_after"] = {name: identity(name) for name in protected_units}
    report["protected_service_identities_unchanged"] = report["protected_services_after"] == protected
    report["sampled_prior_files_unchanged"] = all(
        Path(item["path"]).is_file() and file_sha(Path(item["path"])) == item["sha256"] for item in samples
    ) if samples else None
    atomic_write_json(args.output, report)
    print(json.dumps({key: report[key] for key in ("state", "restart_requested", "protected_service_identities_unchanged")}))
    return 0 if report["state"] == "canonical_owner_schema_and_status_verified" else 1


def main(argv=None):
    with ExitStack() as resources:
        return _main(argv, resources)


if __name__ == "__main__":
    raise SystemExit(main())
