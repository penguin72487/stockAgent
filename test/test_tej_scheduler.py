from contextlib import closing
import json
from pathlib import Path
from threading import Event

import pytest

from downloader.tej_history import connect
from downloader.tej_scheduler import CONTRACT, WatchPolicy, watch_queue


class InstantWait(Event):
    def __init__(self, *, limit=100):
        super().__init__()
        self.waits = []
        self.limit = limit

    def wait(self, timeout=None):
        self.waits.append(timeout)
        if len(self.waits) >= self.limit:
            self.set()
        return self.is_set()


@pytest.fixture
def queue(tmp_path):
    root = tmp_path / "data_tej"
    with closing(connect(root)) as con, con:
        con.execute("INSERT INTO meta VALUES ('config','{}')")
    config = {"automation": {"enabled": True}, "minimum_export_interval_seconds": 2}
    return root, config


def emit(*args, **kwargs):
    pass


def add_task(root, *, state="blocked", error="unknown_outcome_no_auto_retry"):
    with closing(connect(root)) as con, con:
        con.execute("INSERT INTO tasks(task_id,table_id,kind,phase,priority,request_json,state,last_error_code) "
                    "VALUES ('task','table','download','P1',100,'{}',?,?)", (state, error))


def test_watch_continues_without_finite_batch_cap_and_releases_lock(queue):
    root, config = queue
    stop = InstantWait()
    calls = []

    def runner(root, bridge):
        calls.append(1)
        return "completed_task"

    assert watch_queue(root, None, config, stop=stop, runner=runner, max_cycles=4, emit=emit) == 0
    assert len(calls) == 4 and stop.waits == [2] * 4
    status = json.loads((root / "scheduler_status.json").read_text())
    assert status["contract"] == CONTRACT and status["continuous"] is True
    assert status["state"] == "stopped" and status["completed_tasks"] == 4
    assert status["unknown_outcome_auto_retry"] is False
    assert status["query_deadline_renewed_by_heartbeat"] is False
    from downloader.dataset_lock import exclusive_dataset_lock
    with exclusive_dataset_lock(root / ".download.lock", provider="test", timeout_seconds=0):
        pass


def test_safe_input_retry_is_waited_and_then_actually_runs(queue):
    root, config = queue
    stop = InstantWait()
    replies = iter(["prequery_retry_scheduled", "completed_task", "idle"])
    assert watch_queue(root, None, config, stop=stop, runner=lambda *_: next(replies), max_cycles=3, emit=emit) == 0
    assert stop.waits == [5, 2, 30, 30]
    assert json.loads((root / "scheduler_status.json").read_text())["completed_tasks"] == 1


def test_foreground_availability_is_waited_without_spin_or_unknown_retry(queue):
    root, config = queue
    replies = iter(['desktop_unavailable', 'completed_task'])
    stop = InstantWait()
    watch_queue(root, None, config, stop=stop, runner=lambda *_: next(replies), max_cycles=2, emit=emit)
    assert stop.waits == [30,30,2]
    assert json.loads((root/'scheduler_status.json').read_text())['completed_tasks'] == 1


def test_shutdown_finishes_current_bounded_task_before_leaving_queue(queue):
    root, config = queue
    stop = Event()
    def runner(*_):
        stop.set()
        return 'completed_task'
    watch_queue(root, None, config, stop=stop, runner=runner, emit=emit)
    status = json.loads((root/'scheduler_status.json').read_text())
    assert status['state'] == 'stopped' and status['completed_tasks'] == 1


def test_explicit_private_pins_are_reloaded_only_at_safe_task_boundary(queue):
    root, config = queue
    from types import SimpleNamespace
    bridge = SimpleNamespace(session={})
    pins = {'TejProcessId':42,'ExpectedWindow':123,'ExpectedTitle':'TEJ Smart Wizard (Version 4.1.1.7) -- Book2',
            'ExpectedWorkbook':'Book2','ExpectedExcelWindow':90}
    path = root/'desktop_session.json';path.write_text(json.dumps(pins))
    calls = []
    watch_queue(root, bridge, config, stop=InstantWait(), session_path=path,
                runner=lambda *_: calls.append(bridge.session.copy()) or 'completed_task', max_cycles=1, emit=emit)
    assert calls == [pins]


@pytest.mark.parametrize("error", ["unknown_outcome_no_auto_retry", "source_validation_failed", "local_storage_failed",
    "date_input_prequery_needs_review", "list_selection_prequery_needs_review", "query_activation_prequery_needs_review"])
def test_restart_retains_durable_safety_pause_and_never_calls_provider(queue, error):
    root, config = queue
    add_task(root, error=error)
    stop = InstantWait(limit=4)
    calls = []
    watch_queue(root, None, config, stop=stop, runner=lambda *_: calls.append(1), emit=emit)
    assert calls == []
    with closing(connect(root)) as con:
        row = con.execute("SELECT state,last_error_code FROM tasks").fetchone()
        assert tuple(row) == ("blocked", error)
    status = json.loads((root / "scheduler_status.json").read_text())
    assert status["completed_tasks"] == 0 and status["paused_reason"]


def test_running_query_is_not_reset_by_restart(queue):
    root, config = queue
    add_task(root, state="running", error=None)
    stop = InstantWait(limit=2)
    watch_queue(root, None, config, stop=stop, runner=lambda *_: pytest.fail("must not resend"), emit=emit)
    with closing(connect(root)) as con:
        assert con.execute("SELECT state FROM tasks").fetchone()[0] == "running"


def test_interface_barrier_is_never_cleared_by_supervision(queue):
    root, config = queue
    with closing(connect(root)) as con, con:
        con.execute("INSERT INTO meta VALUES ('desktop_interface_recovery_required','{}')")
    watch_queue(root, None, config, stop=InstantWait(limit=2), runner=lambda *_: pytest.fail("must not query"), emit=emit)
    with closing(connect(root)) as con:
        assert con.execute("SELECT 1 FROM meta WHERE key='desktop_interface_recovery_required'").fetchone()


def test_month_axis_replan_barrier_survives_restart_without_repeating_metadata_or_preview(queue):
    root, config = queue
    with closing(connect(root)) as con, con:
        con.execute("INSERT INTO meta VALUES ('source_period_replan_required','exact_completed_task')")
    watch_queue(root,None,config,stop=InstantWait(limit=2),runner=lambda *_:pytest.fail('must not query'),emit=emit)
    status=json.loads((root/'scheduler_status.json').read_text())
    assert status['paused_reason']=='source_period_replan_required' and status['completed_tasks']==0


def test_metadata_only_failed_table_does_not_block_other_work(queue):
    root, config = queue
    add_task(root, error="vendor_metadata_allocation_failed_deferred")
    calls = []
    watch_queue(root, None, config, stop=InstantWait(), runner=lambda *_: calls.append(1) or "completed_task", max_cycles=2, emit=emit)
    assert len(calls) == 2


def test_storage_wait_uses_sleep_not_a_repeating_query(queue):
    root, config = queue
    calls = []
    watch_queue(root, None, config, stop=InstantWait(), runner=lambda *_: calls.append(1) or "local_disk_headroom_low", max_cycles=2, emit=emit)
    assert len(calls) == 2
    assert json.loads((root / "scheduler_status.json").read_text())["completed_tasks"] == 0


@pytest.mark.parametrize("field,value", [("idle_poll_seconds", 0), ("blocked_poll_seconds", float("nan")),
    ("heartbeat_seconds", True), ("heartbeat_seconds", 301)])
def test_invalid_automation_intervals_cannot_spin_or_run(queue, field, value):
    _, config = queue
    config["automation"][field] = value
    with pytest.raises(ValueError):
        WatchPolicy.from_config(config)


def test_automation_requires_explicit_enable():
    with pytest.raises(ValueError):
        WatchPolicy.from_config({})


def test_service_has_graceful_stop_low_background_priority_and_no_cpu_quota():
    repo = Path(__file__).resolve().parents[1]
    unit = (repo / "deploy/systemd/stockagent-tej-history.service.in").read_text()
    assert "Restart=on-failure" in unit and "TimeoutStopSec=1900s" in unit
    assert "KillMode=mixed" in unit and "Nice=10" in unit and "CPUQuota" not in unit
    script = (repo / "scripts/run_tej_history.sh").read_text()
    assert "resolve_fintech_python" in script and "runtime_env.sh" in script and "WSL_INTEROP" in script
    assert "download_tej_history watch" in script
