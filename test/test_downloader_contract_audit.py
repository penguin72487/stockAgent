from __future__ import annotations

from pathlib import Path
import subprocess
import sys

from scripts import audit_downloader_contracts as audit


ROOT = Path(__file__).resolve().parents[1]


def test_direct_cli_imports_shared_runtime_parser_without_pythonpath(tmp_path):
    program = """
from pathlib import Path
import runpy, subprocess, sys
namespace = runpy.run_path(sys.argv[1], run_name='inventory_cli_smoke')
def systemctl(argv, **kwargs):
    output = ('stockagent-source.service static enabled\\n' if argv[1] == 'list-unit-files'
              else 'Id=stockagent-source.service\\nActiveState=inactive\\n')
    return subprocess.CompletedProcess(argv, 0, stdout=output, stderr='')
namespace['subprocess'].run = systemctl
snapshot, references = namespace['runtime_schedule_snapshot'](Path(sys.argv[2]), [])
assert snapshot['units']['stockagent-source.service']['ActiveState'] == 'inactive'
assert not references
"""
    result = subprocess.run(
        [sys.executable, "-I", "-c", program, str(ROOT / "scripts/audit_downloader_contracts.py"), str(tmp_path)],
        cwd=tmp_path, capture_output=True, text=True, timeout=15,
    )
    assert result.returncode == 0, result.stderr


def test_inventory_includes_module_and_script_level_download_entrypoints() -> None:
    paths = {
        path.relative_to(ROOT).as_posix()
        for path in audit.discover_python_entrypoints(ROOT)
    }

    assert "downloader/download_fred_crypto_macro_vintages.py" in paths
    assert "scripts/download_taifex_public_history.py" in paths
    assert "scripts/refresh_tw_public_release_archives.py" in paths
    assert "scripts/process_openbb_archive.py" in paths
    assert "scripts/audit_downloader_contracts.py" not in paths


def test_audit_sees_shared_transport_atomic_receipt_and_daily_registration() -> None:
    runner = (ROOT / "downloader/run_daily_all_markets.sh").read_text(encoding="utf-8")
    row = audit._audit_file(
        ROOT / "downloader/download_fred_crypto_macro_vintages.py",
        ROOT,
        daily_runner=runner,
    )

    assert row.networked is True
    assert row.has_shared_rate_limiter is True
    assert row.has_atomic_publication is True
    assert row.has_receipt_or_manifest is True
    assert row.scheduled_by_daily_runner is True
    assert row.provider_profiles == "fred_api"
    assert row.credential_env_vars == "FRED_API_KEY"
    assert row.contract_grade == "contract_visible"


def test_runtime_inventory_follows_wrappers_and_preserves_every_calendar_rule(tmp_path, monkeypatch):
    (tmp_path / "scripts").mkdir()
    (tmp_path / "downloader").mkdir()
    collector = tmp_path / "downloader/download_source.py"
    collector.write_text('url = "https://example.test/data"\n')
    wrapper = tmp_path / "scripts/run_source.sh"
    wrapper.write_text('run_fintech_python -m downloader.download_source\n')
    calls = []

    def systemctl(argv, **_kwargs):
        calls.append(argv)
        if argv[1] == "list-unit-files":
            stdout = ("stockagent-source.service static enabled\nstockagent-source.timer enabled enabled\n"
                      "stockagent-template@.service disabled enabled\n"
                      "stockagent-agent-benchmark.service transient -\n")
        else:
            assert "stockagent-template@.service" not in argv
            assert "stockagent-agent-benchmark.service" not in argv
            stdout = (
                "Id=stockagent-source.service\nActiveState=failed\nResult=exit-code\n"
                "ExecStart=/usr/bin/bash scripts/run_source.sh --key=PRIVATE_TOKEN\n\n"
                "Id=stockagent-source.timer\nActiveState=active\n"
                "TimersCalendar=first release\nTimersCalendar=second release\n\n"
            )
        return subprocess.CompletedProcess(argv, 0, stdout=stdout, stderr="")

    monkeypatch.setattr(audit.subprocess, "run", systemctl)
    snapshot, references = audit.runtime_schedule_snapshot(tmp_path, [collector])
    assert len(calls) == 2
    assert references["downloader/download_source.py"] == {"stockagent-source.service"}
    assert snapshot["units"]["stockagent-source.timer"]["TimersCalendar"] == "first release\nsecond release"
    assert snapshot["failed_services"] == ["stockagent-source.service"]
    assert "PRIVATE_TOKEN" not in str(snapshot)
    assert "ExecStart" not in snapshot["units"]["stockagent-source.service"]


def test_runtime_inventory_does_not_present_failed_systemctl_as_an_empty_healthy_schedule(tmp_path, monkeypatch):
    import pytest
    def unavailable(*_args, **_kwargs):
        raise subprocess.CalledProcessError(1, "systemctl")
    monkeypatch.setattr(audit.subprocess, "run", unavailable)
    with pytest.raises(subprocess.CalledProcessError):
        audit.runtime_schedule_snapshot(tmp_path, [])


def test_python_wrapper_references_include_relative_and_from_package_imports(tmp_path):
    wrapper = tmp_path / "downloader/download_source_1m.py"
    paths = [wrapper, tmp_path / "downloader/download_source_15m.py",
             tmp_path / "downloader/common.py", tmp_path / "scripts/run_source.py"]
    references = audit.script_references(
        "from .download_source_15m import main\nfrom . import common\n"
        "from scripts import run_source\n", tmp_path, paths, source_path=wrapper,
    )
    assert references == {"downloader/download_source_15m.py", "downloader/common.py",
                          "scripts/run_source.py"}
