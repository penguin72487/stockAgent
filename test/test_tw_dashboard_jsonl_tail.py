from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
import json
import os
from pathlib import Path
import threading

import pytest

from stockagent.live import tw_day_trade_dashboard as dashboard


@pytest.fixture(autouse=True)
def isolated_tail_cache(monkeypatch):
    monkeypatch.setattr(dashboard, "_TAIL_CACHE", {})


@pytest.mark.parametrize("separator", (b"\n", b"\r\n"))
@pytest.mark.parametrize("trailing_newline", (False, True))
@pytest.mark.parametrize("maximum_rows", (1, 2, 3, 7, 100))
def test_streamed_tail_matches_physical_line_semantics(
    tmp_path, separator, trailing_newline, maximum_rows,
):
    encoded = separator.join(
        [b'{"i": 0}', b"", b"null", b' {"i": 1}', b"[]", b" ",
         json.dumps({"nested": [None, "雪", False]}).encode()]
    ) + (separator if trailing_newline else b"")
    path = tmp_path / "rows.jsonl"
    path.write_bytes(encoded)
    expected = [json.loads(line) for line in encoded.splitlines()[-maximum_rows:] if line.strip()]
    expected = [row for row in expected if isinstance(row, dict)]
    assert dashboard._tail(path, maximum_rows) == expected
    assert dashboard._tail(path, maximum_rows) == expected


def test_streamed_tail_crosses_blocks_without_retaining_discarded_fields(tmp_path, monkeypatch):
    path = tmp_path / "large.jsonl"
    raw = [{"i": i, "unused_transport": "x" * (1_100_000 if i == 3 else 400_000)}
           for i in range(7)]
    path.write_text("".join(json.dumps(row) + "\n" for row in raw))
    seen = []

    def project(row):
        seen.append(row["i"])
        return {"i": row["i"]}

    result = dashboard._tail(path, 5, row_projection=project)
    assert result == [{"i": i} for i in range(2, 7)]
    assert seen == list(range(2, 7))
    assert all("unused_transport" not in row for _, rows in dashboard._TAIL_CACHE.values() for row in rows)
    # Projected and unprojected callers must not share an incompatible cache.
    assert dashboard._tail(path, 1) == [raw[-1]]


def test_identical_cold_tail_reads_join_one_load(tmp_path, monkeypatch):
    path = tmp_path / "rows.jsonl"
    path.write_text('{"i": 1}\n')
    original_read = dashboard._read_jsonl_tail
    original_lock = dashboard._shared_query_lock
    entered = threading.Event()
    second_requested = threading.Event()
    release = threading.Event()
    counts = {"reads": 0, "locks": 0}
    guard = threading.Lock()

    def lock(key):
        with guard:
            counts["locks"] += 1
            if counts["locks"] == 2:
                second_requested.set()
        return original_lock(key)

    def read(*args):
        counts["reads"] += 1
        entered.set()
        assert release.wait(5)
        return original_read(*args)

    monkeypatch.setattr(dashboard, "_shared_query_lock", lock)
    monkeypatch.setattr(dashboard, "_read_jsonl_tail", read)
    with ThreadPoolExecutor(max_workers=2) as pool:
        first = pool.submit(dashboard._tail, path, 10)
        assert entered.wait(5)
        second = pool.submit(dashboard._tail, path, 10)
        assert second_requested.wait(5)
        release.set()
        assert first.result(5) == second.result(5) == [{"i": 1}]
    assert counts["reads"] == 1


def test_different_tail_sources_do_not_wait_for_a_global_lock(tmp_path, monkeypatch):
    paths = [tmp_path / f"{i}.jsonl" for i in range(2)]
    for i, path in enumerate(paths):
        path.write_text(json.dumps({"i": i}) + "\n")
    original = dashboard._read_jsonl_tail
    barrier = threading.Barrier(2, timeout=5)

    def read(*args):
        barrier.wait()
        return original(*args)

    monkeypatch.setattr(dashboard, "_read_jsonl_tail", read)
    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = [pool.submit(dashboard._tail, path, 10) for path in paths]
        assert [future.result(5) for future in futures] == [[{"i": 0}], [{"i": 1}]]


@pytest.mark.parametrize("change", ("append", "replace", "rewrite_preserved_mtime"))
def test_tail_invalidates_source_generation(tmp_path, change):
    path = tmp_path / "rows.jsonl"
    path.write_text('{"i": 1}\n')
    assert dashboard._tail(path, 1) == [{"i": 1}]
    before = path.stat()
    if change == "append":
        with path.open("a") as handle:
            handle.write('{"i": 2}\n')
    elif change == "replace":
        replacement = tmp_path / "next.jsonl"
        replacement.write_text('{"i": 2}\n')
        os.replace(replacement, path)
    else:
        path.write_text('{"i": 2}\n')
        os.utime(path, ns=(before.st_atime_ns, before.st_mtime_ns))
    assert dashboard._tail(path, 1) == [{"i": 2}]


def test_concurrent_append_is_not_consumed_or_cached_as_the_previous_generation(tmp_path):
    path = tmp_path / "rows.jsonl"
    path.write_text('{"i": 1}\n')
    appended = False

    def project(row):
        nonlocal appended
        if not appended:
            appended = True
            with path.open("a") as handle:
                handle.write('{"i": 2}\n')
        return row

    assert dashboard._tail(path, 10, row_projection=project) == [{"i": 1}]
    assert not dashboard._TAIL_CACHE
    assert dashboard._tail(path, 10, row_projection=project) == [{"i": 1}, {"i": 2}]


def test_corrupt_tail_is_rejected_not_cached_or_marked_ready(tmp_path):
    path = tmp_path / "rows.jsonl"
    path.write_text('{"i": 1}\n{"unfinished":')
    with pytest.raises(json.JSONDecodeError):
        dashboard._tail(path, 10)
    assert not dashboard._TAIL_CACHE


def test_missing_empty_and_disabled_tail(tmp_path):
    path = tmp_path / "rows.jsonl"
    assert dashboard._tail(path, 10) == []
    path.touch()
    assert dashboard._tail(path, 10) == []
    assert dashboard._tail(path, 0) == []
    assert dashboard._tail(path, -1) == []


@pytest.mark.parametrize("separator", (b"\n", b"\r\n"))
@pytest.mark.parametrize("maximum_rows", (1, 2, 3, 8, 100))
@pytest.mark.parametrize("terminated", (False, True))
def test_verified_append_retains_exact_physical_window(tmp_path, separator, maximum_rows, terminated):
    path = tmp_path / "append.jsonl"
    raw = separator.join([b'{"i":0}', b"", b"null", b'{"i":1}', b"[]", b" ", b'{"i":2}'])
    path.write_bytes(raw + (separator if terminated else b""))
    dashboard._tail(path, maximum_rows, verified_append=True)
    # An unfinished valid physical line is re-read, including split CRLF.
    for suffix in (separator if not terminated else b"", b'{"i":3}' + separator,
                   separator + b' {"i":4}', b" " + separator + b"null" + separator,
                   separator.join([b'{"i":5}'] * 12) + separator):
        with path.open("ab") as handle:
            handle.write(suffix)
        expected = dashboard._tail(path, maximum_rows)
        actual = dashboard._tail(path, maximum_rows, verified_append=True)
        assert actual == expected
        for _, rows in dashboard._TAIL_CACHE.values():
            if isinstance(rows, dashboard._VerifiedTailRows):
                assert len(rows.line_slots) <= maximum_rows
                import hashlib
                assert rows.prefix_sha256 == hashlib.sha256(path.read_bytes()).hexdigest()


def test_verified_append_decodes_only_new_rows_after_all_prefix_bytes_match(tmp_path):
    path = tmp_path / "append.jsonl"
    path.write_bytes(b'{"i":0}\n\nnull\n{"i":1}\n')
    decoded = []
    def decode(line):
        decoded.append(line)
        return json.loads(line)
    assert dashboard._tail(path, 4, row_decoder=decode, verified_append=True) == [{"i": 0}, {"i": 1}]
    assert len(decoded) == 3
    decoded.clear()
    with path.open("ab") as handle:
        handle.write(b'{"i":2}\n')
    assert dashboard._tail(path, 4, row_decoder=decode, verified_append=True) == [{"i": 1}, {"i": 2}]
    assert decoded == [b'{"i":2}\n']


@pytest.mark.parametrize("change", ("rewrite_and_grow", "replace", "truncate"))
def test_verified_append_rejects_changed_old_prefix_or_lineage(tmp_path, change):
    path = tmp_path / "append.jsonl"
    path.write_bytes(b'{"i":0}\n{"i":1}\n{"i":2}\n')
    assert len(dashboard._tail(path, 3, verified_append=True)) == 3
    before = path.stat()
    replacement = b'{"i":8}\n{"i":9}\n{"i":2}\n{"i":3}\n'
    if change == "replace":
        next_path = tmp_path / "replacement.jsonl"
        next_path.write_bytes(replacement)
        os.replace(next_path, path)
    else:
        path.write_bytes(replacement if change == "rewrite_and_grow" else b'{"i":7}\n')
        os.utime(path, ns=(before.st_atime_ns, before.st_mtime_ns))
    assert dashboard._tail(path, 3, verified_append=True) == dashboard._tail(path, 3)


def test_verified_append_skips_invalid_new_lines_outside_requested_tail(tmp_path):
    path = tmp_path / "append.jsonl"
    path.write_bytes(b'{"i":0}\n{"i":1}\n')
    dashboard._tail(path, 2, verified_append=True)
    with path.open("ab") as handle:
        handle.write(b'not-json\n{"i":2}\n{"i":3}\n')
    assert dashboard._tail(path, 2, verified_append=True) == [{"i": 2}, {"i": 3}]


def test_verified_append_rejects_corruption_of_previously_unterminated_line(tmp_path):
    path = tmp_path / "append.jsonl"
    path.write_bytes(b'{"i":0}')
    dashboard._tail(path, 2, verified_append=True)
    with path.open("ab") as handle:
        handle.write(b'{"i":1}\n')
    with pytest.raises(json.JSONDecodeError):
        dashboard._tail(path, 2, verified_append=True)


def test_verified_append_concurrent_growth_is_bounded_and_not_cached(tmp_path):
    path = tmp_path / "append.jsonl"
    path.write_bytes(b'{"i":0}\n')
    def decode(line):
        row = json.loads(line)
        if row["i"] == 1:
            with path.open("ab") as handle:
                handle.write(b'{"i":2}\n')
        return row
    dashboard._tail(path, 3, row_decoder=decode, verified_append=True)
    with path.open("ab") as handle:
        handle.write(b'{"i":1}\n')
    assert dashboard._tail(path, 3, row_decoder=decode, verified_append=True) == [{"i": 0}, {"i": 1}]
    assert all(signature[2] != path.stat().st_size for signature, _ in dashboard._TAIL_CACHE.values())


def test_tail_cache_remains_bounded(tmp_path, monkeypatch):
    monkeypatch.setattr(dashboard, "_TAIL_CACHE_MAX_ENTRIES", 2)
    for i in range(5):
        path = tmp_path / f"{i}.jsonl"
        path.write_text(json.dumps({"i": i}) + "\n")
        assert dashboard._tail(path, 10) == [{"i": i}]
        assert len(dashboard._TAIL_CACHE) <= 2


def test_failed_attempt_projection_preserves_the_entire_summary_contract(tmp_path: Path):
    day = "2026-10-22"
    rows = []
    for error_type, message in (
        ("TimeoutError", "source timeout"),
        ("TWPublicSourceUpdatePending", "source unavailable"),
        ("TWTradingUniversePending", "official scope pending"),
        ("RuntimeError", "same-session TW model input coverage is not ready; active=10 observed_opens=4 observed_quotes=8 required_inputs=9 observed_no_prints=2"),
    ):
        rows.append({
            "source": "discord_scheduled_signal", "status": "failed",
            "session_date": day, "market": "mode-a", "recorded_at": f"{day}T09:00:01+08:00",
            "error_type": error_type, "error_message": message, "error_code": "waiting_source",
            "failure_stage": "input_coverage", "quote_transport": {"universe": "x" * 100_000},
            "future_field": [1, 2, 3],
        })
    rows.append({**rows[0], "opening_input_coverage": {
        "active": 10, "opens": 4, "responses": 8, "required": 9, "no_prints": 2,
        "official_trading_count": 10, "denominator_contract": "official_current_trading_symbols_in_model_v1",
    }})
    ready = {
        "source": "discord_scheduled_signal", "status": "ready", "market": "mode-a",
        "session_date": day, "signal_id": "signal-a", "gate_at": f"{day}T09:00:00+08:00",
        "signal_started_at": f"{day}T09:00:01+08:00", "signal_ready_at": f"{day}T09:00:02+08:00",
        "ready_from_open_ms": 2000, "source_ready_from_open_ms": 700,
        "source_ready_to_signal_ms": 1300, "stages": {"model_inference_ms": 100},
        "quote_transport": {"every_field": {"units": [None, 100, "shares"]}},
        "price_receipt_timing": {"complete": True}, "state_publication": {"total_ms": 12},
        "previous_signal_history_disabled": True,
    }
    rows.extend([ready, {**ready, "source": "executor", "result": "registered",
                         "ledger_persisted_at": f"{day}T09:00:03+08:00",
                         "input_to_ledger_ms": 2300, "ready_to_ledger_ms": 1000},
                 {**rows[0], "session_date": "2026-10-21"},
                 {"source": "ignored", "status": "failed", "market": "mode-a"}])
    path = tmp_path / "opening.jsonl"
    original = "".join(json.dumps(row) + "\n" for row in rows).encode()
    path.write_bytes(original)
    projected = dashboard._tail(path, 10_000, row_projection=dashboard._opening_latency_row_projection)
    for session_date in ("2026-10-21", day, "2026-10-23"):
        args = {"expected_markets": ["mode-a", "mode-b"], "session_date": session_date}
        assert dashboard._opening_signal_latency_summary(projected, **args) == dashboard._opening_signal_latency_summary(rows, **args)
    assert projected[0].keys() == rows[0].keys() - {"quote_transport", "future_field"}
    assert projected[5] == ready
    assert path.read_bytes() == original


@pytest.mark.parametrize("native", (False, True))
@pytest.mark.parametrize("payload", (
    {"source": "discord_scheduled_signal", "status": "failed", "market": "mode", "error_code": None,
     "quote_transport": {"unused": [None, 1, "雪"]}},
    {"source": "discord_scheduled_signal", "market": "mode", "error_type": "TimeoutError"},
    {"source": "discord_scheduled_signal", "status": "ready", "future_field": [1, 2]},
    {"source": "executor", "result": "registered", "quote_transport": {"full": True}},
    {"status": "failed", "unrecognized_source": True},
    {"source": "discord_scheduled_signal", "status": "failed", "unused": float("nan")},
    {"source": "discord_scheduled_signal", "status": "failed", "opening_input_coverage": {"active": 2**100}},
    [], None, "string root", 14,
))
def test_partial_decoder_preserves_missing_null_and_ready_fields(monkeypatch, native, payload):
    if native and dashboard._OPENING_FAILURE_DECODER is None:
        pytest.skip("optional native JSON decoder is unavailable")
    if not native:
        monkeypatch.setattr(dashboard, "_OPENING_FAILURE_DECODER", None)
    encoded = json.dumps(payload).encode()
    expected = dashboard._opening_latency_row_projection(payload) if isinstance(payload, dict) else payload
    assert dashboard._decode_opening_latency_row(encoded) == expected


@pytest.mark.parametrize("encoded", (
    b'{"source":"discord_scheduled_signal","status":"ready","status":"failed","error_code":1,"error_code":2}',
    b'{"source":"ignored","source":"discord_scheduled_signal","status":"failed","unused":Infinity}',
    b'{"source":"discord_scheduled_signal","status":"failed","quote_transport":{"broken":',
    b'{"source":"discord_scheduled_signal","status":"failed","unused":"\xff"}',
    b'{"source":"discord_scheduled_signal","status":"failed","unused":"\\q"}',
    b'{"source":"discord_scheduled_signal","status":"failed","unused":"a\x01b"}',
    b'{"source":"discord_scheduled_signal","status":"failed","unused":01}',
    b'{"source":"discord_scheduled_signal","status":"failed","unused":{"a":1,}}',
))
def test_partial_decoder_duplicates_and_corrupt_extensions_match_the_original(encoded):
    try:
        expected = json.loads(encoded.decode("utf-8"))
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        with pytest.raises(type(exc)):
            dashboard._decode_opening_latency_row(encoded)
    else:
        assert dashboard._decode_opening_latency_row(encoded) == dashboard._opening_latency_row_projection(expected)


def test_decoder_identity_is_part_of_the_cache_key(tmp_path):
    path = tmp_path / "rows.jsonl"
    path.write_text('{"i":1}\n')

    def decoder(line):
        return {"i": json.loads(line)["i"] + 10}

    assert dashboard._tail(path, 10) == [{"i": 1}]
    assert dashboard._tail(path, 10, row_decoder=decoder) == [{"i": 11}]


def test_production_partial_tail_has_exact_summary_parity_and_keeps_raw_receipt(tmp_path):
    path = tmp_path / "opening_signal_latency.jsonl"
    rows = [
        {"source": "discord_scheduled_signal", "status": "failed", "session_date": "2026-10-22",
         "market": "mode", "error_type": "TimeoutError", "quote_transport": {"unused": "x" * 2_000_000}},
        {"source": "discord_scheduled_signal", "status": "ready", "session_date": "2026-10-22",
         "market": "mode", "signal_id": "id", "gate_at": "2026-10-22T09:00:00+08:00",
         "signal_started_at": "2026-10-22T09:00:00+08:00", "signal_ready_at": "2026-10-22T09:00:01+08:00",
         "quote_transport": {"full_ready_contract": True}},
    ]
    original = "".join(json.dumps(row) + "\n" for row in rows).encode()
    path.write_bytes(original)
    projected = dashboard._tail(path, 10_000, row_decoder=dashboard._decode_opening_latency_row)
    args = {"expected_markets": ["mode"], "session_date": "2026-10-22"}
    assert dashboard._opening_signal_latency_summary(projected, **args) == dashboard._opening_signal_latency_summary(rows, **args)
    assert "quote_transport" not in projected[0]
    assert projected[1] == rows[1]
    assert path.read_bytes() == original
