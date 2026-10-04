from __future__ import annotations

import json

import pytest

from scripts import benchmark_tw_day_trade_signal_page as benchmark


def _args(tmp_path):
    state_dir = tmp_path / "state"
    state_dir.mkdir()
    (state_dir / "signals.jsonl").write_text("\n", encoding="utf-8")
    (state_dir / "state.json").write_text("{}\n", encoding="utf-8")
    cache_dir = tmp_path / "cache"
    cache_dir.mkdir()
    return [
        "--state-dir", str(state_dir), "--cache-dir", str(cache_dir),
        "--start-date", "2026-02-25", "--end-date", "2026-09-25",
        "--expected-total", "2",
    ]


def test_signal_benchmark_requires_gateway_projection_directory(tmp_path, monkeypatch):
    args = _args(tmp_path)
    args[3] = str(tmp_path / "missing")
    monkeypatch.setattr(
        benchmark, "build_dashboard_signal_page",
        lambda **_kwargs: pytest.fail("builder must not run without cache directory"),
    )
    with pytest.raises(SystemExit, match="2"):
        benchmark.main(args)


def test_signal_benchmark_records_complete_exact_page(tmp_path, monkeypatch, capsys):
    args = _args(tmp_path)

    def build(**kwargs):
        kwargs["timing_ms"]["source_projection"] = 1.5
        return {
            "total": 2, "returned": 1, "scan_limit_reached": False,
            "opening_execution_audit_scope": "complete_current_signal_rows_per_mode",
            "rows": [{"symbol": "2330"}],
        }

    monkeypatch.setattr(benchmark, "build_dashboard_signal_page", build)
    assert benchmark.main(args) == 0
    result = json.loads(capsys.readouterr().out)
    assert result["complete"] is True
    assert result["source_unchanged"] is True
    assert len(result["signals_signature"]) == 5
    assert len(result["state_signature"]) == 5
    assert result["total"] == 2
    assert result["stages_ms"]["source_projection"] == 1.5
    assert len(result["result_sha256"]) == 64


def test_signal_benchmark_rejects_capped_or_changed_source(tmp_path, monkeypatch, capsys):
    args = _args(tmp_path)

    def capped(**_kwargs):
        return {
            "total": 2, "returned": 1, "scan_limit_reached": True,
            "opening_execution_audit_scope": "bounded_recent_signal_rows_per_mode",
        }

    monkeypatch.setattr(benchmark, "build_dashboard_signal_page", capped)
    assert benchmark.main(args) == 2
    assert json.loads(capsys.readouterr().out)["complete"] is False

    def changed(**_kwargs):
        with (tmp_path / "state/signals.jsonl").open("a", encoding="utf-8") as stream:
            stream.write("changed\n")
        return {
            "total": 2, "returned": 1, "scan_limit_reached": False,
            "opening_execution_audit_scope": "complete_current_signal_rows_per_mode",
        }

    monkeypatch.setattr(benchmark, "build_dashboard_signal_page", changed)
    assert benchmark.main(args) == 2
    result = json.loads(capsys.readouterr().out)
    assert result["source_unchanged"] is False
    assert result["complete"] is False


def test_narrow_probe_requires_full_baseline_hash(tmp_path, monkeypatch):
    args = _args(tmp_path) + ["--probe-narrow-summary"]
    monkeypatch.setattr(
        benchmark, "_narrow_summary_probe",
        lambda **_kwargs: pytest.fail("probe must not run without a baseline hash"),
    )
    with pytest.raises(SystemExit, match="2"):
        benchmark.main(args)


def test_narrow_probe_reports_only_verified_summary(tmp_path, monkeypatch, capsys):
    direction = {"target": {"long_count": 1}}
    audit = {"mode_a": {"model_signal_row_count": 2}}
    expected = benchmark._summary_sha256(direction, audit)
    args = _args(tmp_path) + [
        "--probe-narrow-summary", "--expected-summary-sha", expected,
    ]
    monkeypatch.setattr(
        benchmark, "_narrow_summary_probe",
        lambda **_kwargs: (2, direction, audit, {"narrow_summary": 4.0}),
    )
    assert benchmark.main(args) == 0
    result = json.loads(capsys.readouterr().out)
    assert result["complete"] is True
    assert result["variant"] == "narrow_summary_probe"
    assert result["result_sha256"] is None
    assert result["summary_sha256"] == expected
    assert result["stages_ms"]["narrow_summary"] == 4.0

    args[-1] = "0" * 64
    assert benchmark.main(args) == 2
    assert json.loads(capsys.readouterr().out)["complete"] is False
