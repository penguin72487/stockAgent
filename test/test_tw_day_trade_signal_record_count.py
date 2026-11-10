import json
import threading

from stockagent.live import tw_day_trade_dashboard as dashboard
from stockagent.live.public_dashboards import sanitize_tw_signals


def test_large_signal_count_is_nonblocking_and_never_reports_an_old_generation(tmp_path, monkeypatch):
    path = tmp_path / "signals.jsonl"
    path.write_text('{"a":1}\n', encoding="utf-8")
    monkeypatch.setattr(dashboard, "_SIGNAL_LINE_COUNT_SYNC_MAX_BYTES", 1)
    real_count = dashboard._line_count
    started, release, finished = threading.Event(), threading.Event(), threading.Event()
    calls = []

    def slow_count(source):
        calls.append(source)
        started.set()
        assert release.wait(5)
        try:
            return real_count(source)
        finally:
            finished.set()

    monkeypatch.setattr(dashboard, "_line_count", slow_count)
    assert dashboard._signal_page_record_count(path) is None
    assert started.wait(2)
    assert dashboard._signal_page_record_count(path) is None
    assert len(calls) == 1
    release.set()
    assert finished.wait(5)
    assert dashboard._signal_page_record_count(path) == 1
    with path.open("a", encoding="utf-8") as handle:
        handle.write('{"a":2}\n')
    # The former exact count is not valid for the appended generation.
    assert dashboard._signal_page_record_count(path) is None


def test_slow_line_count_does_not_block_a_different_journal(tmp_path, monkeypatch):
    signal, fills = tmp_path / "signals.jsonl", tmp_path / "fills.jsonl"
    signal.write_text("one\n", encoding="utf-8")
    fills.write_text("two\n", encoding="utf-8")
    assert dashboard._line_count(signal) == 1
    with signal.open("a", encoding="utf-8") as handle:
        handle.write("three\n")
    started, release = threading.Event(), threading.Event()
    actual = dashboard._hash_jsonl_prefix

    def slow_prefix(handle, count):
        started.set()
        assert release.wait(5)
        return actual(handle, count)

    monkeypatch.setattr(dashboard, "_hash_jsonl_prefix", slow_prefix)
    result = []
    worker = threading.Thread(target=lambda: result.append(dashboard._line_count(signal)))
    worker.start()
    assert started.wait(2)
    try:
        assert dashboard._line_count(fills) == 1
    finally:
        release.set()
        worker.join(5)
    assert result == [2]


def test_cached_signal_page_promotes_pending_count_only_when_exact(tmp_path, monkeypatch):
    root = tmp_path / "state"
    root.mkdir()
    (root / "state.json").write_text(json.dumps({"modes": {
        "mode_a": {"session_date": "2026-08-14", "initial_capital_twd": 10000000}}}), encoding="utf-8")
    rows = [{"session_date": "2026-08-13", "market": "mode_a", "signal_id": "signal",
             "symbol": symbol, "name": symbol, "target_weight": 0.1, "status": "hold",
             "requested_shares": 0, "filled_shares": 0} for symbol in ("A", "B", "C")]
    (root / "signals.jsonl").write_text("".join(json.dumps(row) + "\n" for row in rows), encoding="utf-8")
    observations = iter([None, 3])
    monkeypatch.setattr(dashboard, "_signal_page_record_count", lambda _path: next(observations))
    request = {"state_dir": root, "session_date": "2026-08-13", "limit": 1}
    pending = dashboard.build_dashboard_signal_page(**request)
    assert pending["record_count"] is None and pending["record_count_pending"] is True
    public = sanitize_tw_signals(pending)
    assert public["record_count"] is None and public["record_count_pending"] is True
    ready = dashboard.build_dashboard_signal_page(**request)
    assert ready["record_count"] == 3 and ready["record_count_pending"] is False
    assert ready["rows"] == pending["rows"]
