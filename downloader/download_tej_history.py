"""Canonical TEJ desktop acquisition CLI. Raw licensed values never serve HTTP."""
from __future__ import annotations

import argparse
from contextlib import closing
from datetime import datetime
import json
from pathlib import Path
import time
import signal
from threading import Event
from zoneinfo import ZoneInfo

from downloader.dataset_lock import exclusive_dataset_lock
from downloader.tej_history import (
    DesktopBridge, compact_queue, configure_runtime_policy, configure_preview_planning, isolate_failed_metadata,
    mark_batch_finished, mark_worker_wait, recover_desktop_response, recover_evidence,
    PREQUERY_FAILURES, recover_legacy_local_input, recover_verified_input, connect, refresh_axis_profiles, register_inventory,
    revalidate_source_scopes, run_one, settle_metadata_failure, settle_preview_column_limit,
    verify_desktop_input,
)


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("action", choices=("register", "run", "watch", "recover", "recover-desktop", "retry-unknown", "repair-key-layout", "repair-period-grid", "reopen-query", "restart-addin", "verify-interface", "compact-queue", "revalidate-scopes", "settle-metadata-error", "isolate-metadata", "recover-local-input", "settle-preview-limit", "upgrade-preview-planning", "upgrade-query-tiling", "verify-input"))
    p.add_argument("--root", type=Path, default=Path("data_tej"))
    p.add_argument("--inventory", type=Path)
    p.add_argument("--config", type=Path, default=Path("configs/tej_history.json"))
    p.add_argument("--session", type=Path, default=Path("data_tej/desktop_session.json"))
    p.add_argument("--cutoff", default=datetime.now(ZoneInfo("Asia/Taipei")).date().isoformat())
    p.add_argument("--max-tasks", type=int, default=1)
    modes=p.add_mutually_exclusive_group()
    modes.add_argument("--discover-only", action="store_true", help="Inventory table axes only; never send a Preview data query")
    modes.add_argument("--download-only", action="store_true", help="Download already verified scopes; do not select new tables for discovery")
    p.add_argument("--task-id")
    p.add_argument('--table-id',help='One exact registered table for download-only acceptance; same canonical queue and lock')
    p.add_argument("--evidence", type=Path)
    p.add_argument('--acknowledge-unknown-usage', action='store_true',
                   help='Explicit one-shot operator replay may consume additional provider usage; never automatic')
    p.add_argument('--allow-discard-query-settings',action='store_true',
                   help='Explicitly allow closing ONLY the scratch Wizard, retaining Excel and all workbooks')
    p.add_argument('--allow-restart-addin',action='store_true',
                   help='Separate operator permission to restart ONLY the exact independently inspected TEJAddin process')
    p.add_argument('--recovery-run',type=Path,
                   help='Reconcile an exact recorded scratch-query close/open or authorized add-in restart; never repeat an unknown launch')
    p.add_argument("--response", choices=("preview", "empty", "excel_error", "plan", "plan-empty-fields", "plan-preparation"))
    p.add_argument("--error-window", type=int)
    a = p.parse_args(argv)
    if not 1 <= a.max_tasks <= 1000:
        p.error("max-tasks must be between 1 and 1000; finite desktop ownership only")
    if (a.discover_only or a.download_only) and a.action != "run":
        p.error("task-kind selectors are only valid with run")
    if a.table_id is not None and (a.action!='run' or not a.download_only):
        p.error('--table-id requires run --download-only')
    root = a.root.resolve()
    if root == root.parent or root in {Path.home().resolve(), Path.cwd().resolve()}:
        p.error("root must be a dedicated acquisition directory, not a home or workspace root")
    config = json.loads(a.config.read_text())
    if a.action == "watch":
        if (a.task_id is not None or a.table_id is not None or a.evidence is not None
                or a.acknowledge_unknown_usage or a.allow_discard_query_settings
                or a.allow_restart_addin or a.recovery_run is not None or a.response is not None
                or a.error_window is not None or a.max_tasks != 1):
            p.error("watch supervises the canonical queue; operator recovery/replay is never automatic")
        from downloader.tej_scheduler import WatchPolicy, watch_queue
        WatchPolicy.from_config(config)
        stop = Event()
        previous_handlers = {sig: signal.getsignal(sig) for sig in (signal.SIGTERM, signal.SIGINT)}
        for sig in previous_handlers:
            signal.signal(sig, lambda _signum, _frame: stop.set())
        try:
            with exclusive_dataset_lock(root / ".scheduler.lock", provider="tej_smart_wizard_automatic", timeout_seconds=0):
                with exclusive_dataset_lock(root / ".download.lock", provider="tej_smart_wizard", timeout_seconds=0):
                    refresh_axis_profiles(root)
                    configure_runtime_policy(root, config)
                    if config.get("download_planning_contract"):
                        configure_preview_planning(root, config)
                bridge = DesktopBridge(Path(__file__).resolve().parents[1], json.loads(a.session.read_text()))
                return watch_queue(root, bridge, config, stop=stop, session_path=a.session.resolve())
        finally:
            for sig, handler in previous_handlers.items():
                signal.signal(sig, handler)
    if a.acknowledge_unknown_usage and a.action!='retry-unknown':
        p.error('--acknowledge-unknown-usage is only valid for explicit retry-unknown')
    if a.allow_discard_query_settings and a.action not in {'reopen-query','restart-addin'}:
        p.error('--allow-discard-query-settings is only valid for explicit query lifecycle recovery')
    if a.allow_restart_addin and a.action!='restart-addin':
        p.error('--allow-restart-addin is only valid for explicit restart-addin')
    if a.recovery_run is not None and a.action not in {'restart-addin','reopen-query'}:
        p.error('--recovery-run is only valid for explicit query lifecycle recovery')
    with exclusive_dataset_lock(root / ".download.lock", provider="tej_smart_wizard", timeout_seconds=0):
        if a.action == "compact-queue":
            print(json.dumps(compact_queue(root)))
        elif a.action == "revalidate-scopes":
            print(json.dumps(revalidate_source_scopes(root)))
        elif a.action == 'verify-input':
            if a.task_id is None:
                p.error('verify-input requires --task-id; re-enter the same dates only, no Search/Preview or recovery')
            bridge=DesktopBridge(Path(__file__).resolve().parents[1],json.loads(a.session.read_text()))
            print(json.dumps(verify_desktop_input(root,a.task_id,bridge)))
        elif a.action == 'retry-unknown':
            if a.task_id is None or a.evidence is None or not a.acknowledge_unknown_usage:
                p.error('retry-unknown requires exact --task-id, original --evidence and --acknowledge-unknown-usage')
            from downloader.tej_desktop_attempts import retry_unknown_download
            configure_runtime_policy(root,config)
            bridge=DesktopBridge(Path(__file__).resolve().parents[1],json.loads(a.session.read_text()))
            result=retry_unknown_download(root,a.task_id,bridge,a.evidence)
            print(json.dumps(result))
            return 0 if result['state']=='completed_task' else 1
        elif a.action=='restart-addin':
            if a.task_id is None or not a.allow_restart_addin or not a.allow_discard_query_settings:
                p.error('restart-addin requires exact --task-id and both explicit --allow-restart-addin/--allow-discard-query-settings')
            from downloader.tej_query_lifecycle import restart_addin
            bridge=DesktopBridge(Path(__file__).resolve().parents[1],json.loads(a.session.read_text()))
            print(json.dumps(restart_addin(root,a.task_id,bridge,a.session,
                allow_restart=a.allow_restart_addin,allow_discard=a.allow_discard_query_settings,recovery_run=a.recovery_run)))
        elif a.action in {'reopen-query','verify-interface'}:
            if a.task_id is None or a.action=='reopen-query' and not a.allow_discard_query_settings:
                p.error('Exact --task-id required; reopen-query also requires explicit --allow-discard-query-settings')
            from downloader.tej_query_lifecycle import adopt_open_query, reopen_query, verify_interface
            bridge=DesktopBridge(Path(__file__).resolve().parents[1],json.loads(a.session.read_text()))
            result=(adopt_open_query(root,a.task_id,bridge,a.session,a.recovery_run) if a.recovery_run is not None else
                    reopen_query(root,a.task_id,bridge,a.session,allow_discard=a.allow_discard_query_settings)
                    if a.action=='reopen-query' else verify_interface(root,a.task_id,bridge))
            print(json.dumps(result))
        elif a.action == 'repair-key-layout':
            if a.task_id is None:
                p.error('repair-key-layout requires one exact rejected/unsent --task-id')
            from downloader.tej_key_layout import repair_source_key_plan
            bridge=DesktopBridge(Path(__file__).resolve().parents[1],json.loads(a.session.read_text()))
            print(json.dumps(repair_source_key_plan(root,a.task_id,bridge)))
        elif a.action == 'repair-period-grid':
            if a.task_id is None:
                p.error('repair-period-grid requires one exact completed YYYYMM --task-id; metadata only, no Preview')
            from downloader.tej_period_keys import replan_month_periods, approve_saved_month_interpretation
            bridge=DesktopBridge(Path(__file__).resolve().parents[1],json.loads(a.session.read_text()))
            if a.evidence is not None:
                approve_saved_month_interpretation(root,a.task_id,a.evidence)
                recover_evidence(root,a.task_id,a.evidence)
            print(json.dumps(replan_month_periods(root,a.task_id,bridge)))
        elif a.action == 'settle-metadata-error':
            if a.task_id is None or a.evidence is None:
                p.error('settle-metadata-error requires --task-id and exact private --evidence')
            bridge=DesktopBridge(Path(__file__).resolve().parents[1],json.loads(a.session.read_text()))
            print(json.dumps(settle_metadata_failure(root,a.task_id,bridge,a.evidence)))
        elif a.action == 'upgrade-query-tiling':
            from downloader.tej_planning import upgrade_query_tiling
            print(json.dumps(upgrade_query_tiling(root,config)))
        elif a.action == 'upgrade-preview-planning':
            print(json.dumps(configure_preview_planning(root,config)))
        elif a.action == 'settle-preview-limit':
            if a.task_id is None or a.evidence is None or a.error_window is None:
                p.error('settle-preview-limit requires --task-id, exact prepared --evidence and independently inspected --error-window')
            bridge=DesktopBridge(Path(__file__).resolve().parents[1],json.loads(a.session.read_text()))
            print(json.dumps(settle_preview_column_limit(root,a.task_id,bridge,a.evidence,a.error_window)))
        elif a.action == 'recover-local-input':
            if a.task_id is None or a.evidence is None:
                p.error('recover-local-input requires --task-id and exact original private download --evidence')
            bridge=DesktopBridge(Path(__file__).resolve().parents[1],json.loads(a.session.read_text()))
            with closing(connect(root)) as con:
                row=con.execute('SELECT last_error_code FROM tasks WHERE task_id=?',(a.task_id,)).fetchone()
            recover=recover_verified_input if row and row[0] in PREQUERY_FAILURES.values() else recover_legacy_local_input
            print(json.dumps(recover(root,a.task_id,bridge,a.evidence)))
        elif a.action == 'isolate-metadata':
            if a.task_id is None or a.evidence is None:
                p.error('isolate-metadata requires --task-id and exact original private plan --evidence')
            bridge=DesktopBridge(Path(__file__).resolve().parents[1],json.loads(a.session.read_text()))
            print(json.dumps(isolate_failed_metadata(root,a.task_id,bridge,a.evidence,
                acknowledge_known_notices=config.get('acknowledge_known_metadata_runtime_notices') is True)))
        elif a.action == "register":
            if a.inventory is None:
                p.error("register requires --inventory")
            print(json.dumps(register_inventory(root, a.inventory, config, cutoff=a.cutoff)))
        elif a.action == "recover":
            if a.task_id is None or a.evidence is None:
                p.error("recover requires --task-id and --evidence; no new provider request")
            recover_evidence(root, a.task_id, a.evidence)
            print(json.dumps({"state": "evidence_recovered"}))
        elif a.action == "recover-desktop":
            if a.task_id is None or a.response is None:
                p.error("recover-desktop requires --task-id and --response; no new Preview")
            bridge = DesktopBridge(Path(__file__).resolve().parents[1], json.loads(a.session.read_text()))
            print(json.dumps(recover_desktop_response(root, a.task_id, bridge, response=a.response,
                                                      error_window=a.error_window)))
        else:
            # Populate old discovery menu bounds once from exact local
            # evidence. The public projection never opens these source files.
            refresh_axis_profiles(root)
            configure_runtime_policy(root,config)
            if config.get('download_planning_contract'):
                configure_preview_planning(root,config)
            bridge = DesktopBridge(Path(__file__).resolve().parents[1], json.loads(a.session.read_text()))
            kind='discover' if a.discover_only else 'download' if a.download_only else None
            for index in range(a.max_tasks):
                state = run_one(root, bridge, kind=kind,table_id=a.table_id)
                print(json.dumps({"state": state}), flush=True)
                if state == "prequery_retry_scheduled":
                    mark_worker_wait(root,kind='download',seconds=5,local_retry=True)
                    time.sleep(5)
                    continue
                if state == 'idle':
                    mark_batch_finished(root,reason='no_ready_tasks_in_selected_mode',attempted=index,mode=kind or 'interleaved')
                    return 0
                if state not in {'completed_task','source_key_layout_replanned','metadata_preparation_failed_deferred','vendor_metadata_allocation_failed_deferred'}:
                    return 1
                mark_worker_wait(root,kind=kind or 'interleaved',
                                 seconds=config["minimum_export_interval_seconds"])
                time.sleep(config["minimum_export_interval_seconds"])
            mark_batch_finished(root,reason='finite_task_limit_reached',attempted=a.max_tasks,mode=kind or 'interleaved')
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
