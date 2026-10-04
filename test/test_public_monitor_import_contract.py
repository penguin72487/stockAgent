from __future__ import annotations

from datetime import UTC, datetime, timedelta
import json
from pathlib import Path
import subprocess
import sys

import pytest

from scripts import benchmark_public_monitor_imports as benchmark
from stockagent.data import finlab_acquisition_contract as finlab


@pytest.mark.parametrize("target", benchmark.TARGETS)
@pytest.mark.parametrize("missing_footer_reader", (False, True))
def test_public_reader_import_rejects_collector_and_sdk_dependencies(target, missing_footer_reader):
    program = """
import importlib
import importlib.abc
import json
import sys
class NoWorkers(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if sys.argv[2] == 'True' and (fullname == 'pyarrow' or fullname.startswith('pyarrow.')):
            raise ImportError('optional footer library unavailable in this import-only test')
        prefixes = ('downloader.download_finmind_', 'scripts.download_finlab_history',
                    'scripts.finlab_arrow_history', 'finlab', 'shioaji', 'pandas', 'requests')
        if any(fullname == prefix or fullname.startswith(prefix + '.')
               or (prefix.endswith('_') and fullname.startswith(prefix)) for prefix in prefixes):
            raise AssertionError('public reader imported worker dependency: ' + fullname)
sys.meta_path.insert(0, NoWorkers())
sys.dont_write_bytecode = True
module = importlib.import_module(sys.argv[1])
print(json.dumps(getattr(module, '_SHARED_IMPORT_OBSERVATION', {})))
"""
    result = subprocess.run(
        [sys.executable, "-c", program, target, str(missing_footer_reader)], cwd=benchmark.ROOT,
        capture_output=True, text=True, timeout=20,
    )
    assert result.returncode == 0, result.stderr
    if target == "scripts.snapshot_data_refresh_services":
        observation = json.loads(result.stdout)
        assert observation["schema_version"] == 1
        assert observation["wall_ms"] >= 0
        assert observation["scope"] == "shared_module_imports_only_excludes_interpreter_and_stdlib_startup"
        assert not any(observation["worker_modules_loaded_at_import_completion"].values())


def test_collectors_and_public_readers_share_rules_not_copies():
    from downloader import download_finmind_complement as collector
    from downloader import finmind_catalog as catalog
    from downloader import finmind_scheduling as scheduling
    from scripts import download_finlab_history as history
    from scripts import finlab_release_gate as gate
    from stockagent.live import data_monitor_dashboard as dashboard
    from stockagent.live import finlab_dashboard

    assert history.safe_stem is gate.safe_stem is finlab_dashboard.safe_stem is finlab.safe_stem
    assert history.attempt_retry_at is dashboard.attempt_retry_at is finlab.attempt_retry_at
    assert history.AUTOMATICALLY_DEFERRED_REASONS is dashboard.AUTOMATICALLY_DEFERRED_REASONS
    assert history.ATTEMPT_RETRY_SECONDS is finlab.ATTEMPT_RETRY_SECONDS
    assert collector.ALL_DATASETS is dashboard.FINMIND_COMPLEMENT_DATASETS is catalog.ALL_DATASETS
    assert collector.SNAPSHOTS is dashboard.FINMIND_COMPLEMENT_SNAPSHOTS is catalog.SNAPSHOTS
    assert collector.WIDE_INSTITUTIONAL is dashboard.FINMIND_DERIVED_WIDE is catalog.WIDE_INSTITUTIONAL
    assert dashboard.FINMIND_SPONSOR_SOURCES is scheduling.SOURCES
    assert dashboard.FINMIND_SESSION_DAY_DATASETS is scheduling.SESSION_DAY_DATASETS
    assert len(catalog.ALL_DATASETS) == len(set(catalog.ALL_DATASETS)) == 69


@pytest.mark.parametrize("streak", (True, False, None, 0, -3, "4", 1))
def test_retry_preserves_invalid_streak_fallback(streak):
    attempted = datetime(2026, 10, 1, 0, tzinfo=UTC)
    assert finlab.attempt_retry_at({
        "status": "provider_error", "failure_streak": streak,
        "attempted_at_utc": attempted.isoformat(),
    }) == attempted + timedelta(minutes=5)


@pytest.mark.parametrize("status", ("authentication_failed", "quota_exhausted", "unknown", None))
def test_account_failures_do_not_gain_per_key_cooldown(status):
    assert finlab.attempt_retry_at({"status": status, "attempted_at_utc": "2026-10-01T00:00:00Z"}) is None


@pytest.mark.parametrize("attempted", (None, "bad", "2026-10-01T00:00:00", 17))
def test_untrusted_retry_clock_does_not_invent_deadline(attempted):
    assert finlab.attempt_retry_at({"status": "timed_out", "attempted_at_utc": attempted}) is None


def test_retry_keeps_downloaded_timeout_and_timezone_semantics():
    attempt = {"status": "timed_out", "failure_streak": 9999,
               "attempted_at_utc": "2026-10-01T08:00:00+08:00"}
    assert finlab.attempt_retry_at(attempt) == datetime(2026, 10, 1, 2, tzinfo=UTC)
    assert finlab.attempt_retry_at(attempt, downloaded=True) == datetime(2026, 10, 1, 1, tzinfo=UTC)


def test_import_baseline_capture_is_once_only_and_checksum_checked(tmp_path: Path):
    path = tmp_path / "baseline.json"
    benchmark.capture_baseline(path)
    assert set(benchmark._read_baseline(path)["modules"]) == set(benchmark.FROZEN_MODULES)
    with pytest.raises(ValueError, match="replace"):
        benchmark.capture_baseline(path)
    payload = json.loads(path.read_text())
    payload["modules"][benchmark.FROZEN_MODULES[0]]["source"] += "\n# changed"
    path.write_text(json.dumps(payload))
    with pytest.raises(ValueError, match="SHA"):
        benchmark._read_baseline(path)


def test_import_probe_never_hides_worker_modules(tmp_path: Path):
    path = tmp_path / "baseline.json"
    benchmark.capture_baseline(path)
    result = subprocess.run([
        sys.executable, str(Path(benchmark.__file__).resolve()),
        "--probe", "stockagent.live.finlab_dashboard", "--baseline", str(path),
    ], capture_output=True, text=True, cwd=benchmark.ROOT, timeout=20)
    assert result.returncode == 0, result.stderr
    payload = json.loads(result.stdout)
    assert payload["process_peak_rss_kib"] > 0
    assert payload["import_wall_ms"] >= 0
    assert payload["import_cpu_ms"] >= 0
    assert len(payload["contract_sha256"]) == 64
    for name in ("finlab", "shioaji", "pandas", "requests", "scripts.download_finlab_history"):
        assert payload["loaded_modules"][name] is False
