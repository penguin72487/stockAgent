"""Run-bound, sanitized source counts in downloader step receipts."""

from datetime import datetime, timezone
import json
from pathlib import Path
import subprocess
import time

from scripts.write_downloader_step_receipt import write_receipt


def test_yahoo_source_summary_is_bound_to_exact_run_and_sanitized(tmp_path):
    run_id = "registered-daily-20260925T010203123456789Z"
    source = tmp_path / "daily_update_summary.us_stocks.json"
    source.write_text(json.dumps({
        "schema_version": 1,
        "run_id": run_id,
        "mode": "daily-update",
        "asset_class": "us_stocks",
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "elapsed_seconds": 123.4,
        "status_counts": {"repaired": 12000, "failed": 12, "lagging_skip": 615},
        "private_command": "--api-key=secret",
    }))
    kwargs = dict(
        receipt_dir=tmp_path / "runs" / run_id,
        latest_dir=tmp_path / "latest",
        run_id=run_id,
        run_mode="once",
        step="yahoo_us_stocks_daily_update",
        started_epoch=time.time() - 2,
        exit_code=0,
        elapsed_seconds=2.0,
        runner_pid=123,
        source_summary=source,
    )
    receipt = write_receipt(**kwargs, state="complete")
    assert receipt["state"] == "complete"
    assert receipt["source_summary_status"] == "matched"
    assert receipt["source_summary"]["unresolved_count"] == 627
    assert receipt["source_summary"]["status_counts"] == {
        "repaired": 12000, "failed": 12, "lagging_skip": 615,
    }
    assert "secret" not in json.dumps(receipt)
    assert json.loads((kwargs["receipt_dir"] / "yahoo_us_stocks_daily_update.json").read_text()) == receipt

    source_payload = json.loads(source.read_text())
    source_payload["run_id"] = "older-run"
    source.write_text(json.dumps(source_payload))
    mismatched = write_receipt(**kwargs, state="failed")
    assert mismatched["source_summary_status"] == "unavailable"
    assert "source_summary" not in mismatched


def test_running_step_does_not_read_previous_source_summary(tmp_path):
    receipt = write_receipt(
        receipt_dir=tmp_path / "run",
        latest_dir=tmp_path / "latest",
        run_id="registered-daily-20260925T010203Z",
        run_mode="once",
        step="yahoo_us_stocks_daily_update",
        state="running",
        started_epoch=time.time(),
        exit_code=None,
        elapsed_seconds=None,
        runner_pid=123,
        source_summary=tmp_path / "missing.json",
    )
    assert "source_summary_status" not in receipt


def test_yahoo_runner_resolves_source_summary_under_nounset_without_network():
    repo_root = Path(__file__).resolve().parents[1]
    command = """
source downloader/run_daily_all_markets.sh
RUN_YAHOO=1
YAHOO_ASSETS=us_stocks
run_step() { printf '%s\\n' "$STEP_SOURCE_SUMMARY_PATH"; }
run_yahoo_incremental
"""
    result = subprocess.run(
        ["bash", "-c", command], cwd=repo_root,
        capture_output=True, text=True, timeout=10, check=False,
    )
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == str(
        repo_root / "data_yahoo/daily_update_summary.us_stocks.json"
    )
