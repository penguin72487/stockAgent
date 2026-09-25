from __future__ import annotations

import json
import subprocess
import sys

import pytest

from scripts import refresh_crypto_training_dataset as refresh


def test_mid_run_raw_writer_defers_without_publishing(tmp_path, monkeypatch) -> None:
    writer_snapshots = iter([[], [], ["321:download_bybit_perp_1m.py"]])
    commands: list[list[str]] = []
    monkeypatch.setattr(refresh, "active_raw_writers", lambda: next(writer_snapshots))
    monkeypatch.setattr(
        refresh.subprocess,
        "run",
        lambda command, **_kwargs: commands.append(command),
    )
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "refresh_crypto_training_dataset.py",
            "--publish",
            "--output-dir",
            str(tmp_path),
        ],
    )

    refresh.main()

    receipt = json.loads((tmp_path / "refresh_receipt.json").read_text())
    assert receipt["state"] == "deferred_raw_writer"
    assert receipt["active_writers"] == ["321:download_bybit_perp_1m.py"]
    assert receipt["finished_at_utc"]
    assert len(receipt["steps"]) == 1
    assert len(receipt["step_attempts"]) == 1
    assert receipt["step_attempts"][0]["status"] == "process_completed"
    assert receipt["step_attempts"][0]["elapsed_seconds"] >= 0
    assert len(commands) == 1
    assert "publish" not in commands[0]


def test_materialization_signature_detects_atomic_source_replacement(tmp_path) -> None:
    source = tmp_path / "data_bybit/1m/BTCUSDT_features.parquet"
    source.parent.mkdir(parents=True)
    source.write_bytes(b"old")
    funding = tmp_path / "data_bybit/funding/BTCUSDT_funding.parquet"
    funding.parent.mkdir(parents=True)
    funding.write_bytes(b"funding")
    (funding.parent / "instruments.csv").write_bytes(b"code\nBTCUSDT\n")
    (funding.parent / "funding_coverage.csv").write_bytes(b"symbol\nBTCUSDT\n")
    first = refresh.materialization_input_signature(tmp_path)
    replacement = source.with_suffix(".new")
    replacement.write_bytes(b"new")
    replacement.replace(source)
    second = refresh.materialization_input_signature(tmp_path)
    assert first["files"] == second["files"] == 4
    assert first["metadata_sha256"] != second["metadata_sha256"]


def test_source_change_during_materialization_defers_without_publishing(
    tmp_path, monkeypatch
) -> None:
    commands: list[list[str]] = []
    signatures = iter(
        [
            {"files": 2, "metadata_sha256": "before"},
            {"files": 2, "metadata_sha256": "after"},
        ]
    )
    monkeypatch.setattr(refresh, "active_raw_writers", lambda: [])
    monkeypatch.setattr(
        refresh, "materialization_input_signature", lambda: next(signatures)
    )
    monkeypatch.setattr(
        refresh.subprocess,
        "run",
        lambda command, **_kwargs: commands.append(command),
    )
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "refresh_crypto_training_dataset.py",
            "--publish",
            "--output-dir",
            str(tmp_path),
        ],
    )

    refresh.main()

    receipt = json.loads((tmp_path / "refresh_receipt.json").read_text())
    assert receipt["state"] == "deferred_source_changed"
    assert receipt["input_signature_before"]["metadata_sha256"] == "before"
    assert receipt["input_signature_after"]["metadata_sha256"] == "after"
    assert len(receipt["steps"]) == 1
    assert len(receipt["step_attempts"]) == 2
    assert all(row["status"] == "process_completed" for row in receipt["step_attempts"])
    assert len(commands) == 2
    assert all("publish" not in command for command in commands)


def test_failed_stage_keeps_elapsed_and_command_in_receipt(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(refresh, "active_raw_writers", lambda: [])

    def fail(command, **_kwargs):
        raise subprocess.CalledProcessError(1, command)

    monkeypatch.setattr(refresh.subprocess, "run", fail)
    monkeypatch.setattr(
        sys, "argv", ["refresh_crypto_training_dataset.py", "--output-dir", str(tmp_path)],
    )

    with pytest.raises(subprocess.CalledProcessError):
        refresh.main()

    receipt = json.loads((tmp_path / "refresh_receipt.json").read_text())
    assert receipt["state"] == "failed"
    assert receipt["steps"] == []
    assert len(receipt["step_attempts"]) == 1
    assert receipt["step_attempts"][0]["status"] == "failed"
    assert receipt["step_attempts"][0]["elapsed_seconds"] >= 0
    assert receipt.get("active_step") is None
