from __future__ import annotations

from pathlib import Path

from scripts import audit_downloader_contracts as audit


ROOT = Path(__file__).resolve().parents[1]


def test_inventory_includes_module_and_script_level_download_entrypoints() -> None:
    paths = {
        path.relative_to(ROOT).as_posix()
        for path in audit.discover_python_entrypoints(ROOT)
    }

    assert "downloader/download_fred_crypto_macro_vintages.py" in paths
    assert "scripts/download_taifex_public_history.py" in paths
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
