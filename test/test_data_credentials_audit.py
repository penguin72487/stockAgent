from __future__ import annotations

import json
import os
from pathlib import Path
import re
import sys
from types import SimpleNamespace

from downloader.openbb_credentials import (
    OPENBB_ENV_TO_CREDENTIAL_FIELD,
    apply_openbb_environment_credentials,
)
from scripts import audit_data_credentials as audit
from scripts.migrate_openbb_credentials_to_env import migrate_credentials


def test_credential_audit_publishes_presence_without_secret_values(
    tmp_path: Path, monkeypatch
) -> None:
    env_file = tmp_path / ".env"
    env_file.write_text(
        "SHIOAJI_API_KEY=very-secret-api-key\n"
        "SHIOAJI_SECRET_KEY=very-secret-secret-key\n"
        "FINNHUB_API_KEY=another-secret\n"
        "FRED_API_KEY=canonical-fred-secret\n",
        encoding="utf-8",
    )
    settings = tmp_path / "user_settings.json"
    settings.write_text(
        json.dumps({"credentials": {"fred_api_key": "fred-secret"}}),
        encoding="utf-8",
    )
    output = tmp_path / "status.json"
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "audit_data_credentials.py",
            "--env-file",
            str(env_file),
            "--openbb-settings",
            str(settings),
            "--output",
            str(output),
        ],
    )

    audit.main()

    raw = output.read_text(encoding="utf-8")
    payload = json.loads(raw)
    assert payload["secret_values_included"] is False
    assert "very-secret" not in raw
    assert "another-secret" not in raw
    assert "fred-secret" not in raw
    assert "canonical-fred-secret" not in raw
    assert next(row for row in payload["providers"] if row["id"] == "shioaji")[
        "state"
    ] == "configured"
    assert next(
        row for row in payload["providers"] if row["id"] == "openbb:fred_api_key"
    )["state"] == "configured"
    assert next(
        row for row in payload["providers"] if row["id"] == "openbb:fred_api_key"
    )["legacy_fallback_configured"] is True
    assert oct(os.stat(output).st_mode & 0o777) == "0o600"


def test_credential_registry_and_examples_reserve_every_declared_slot() -> None:
    root = Path(__file__).resolve().parents[1]
    registry = json.loads(
        (root / "configs/data_api_credentials.json").read_text(encoding="utf-8")
    )
    assert registry["secret_values_permitted"] is False
    providers = registry["providers"]
    assert len(providers) >= 35
    assert len({row["id"] for row in providers}) == len(providers)

    example_names = {
        match.group(1)
        for line in (root / ".env.example").read_text(encoding="utf-8").splitlines()
        if (match := re.match(r"^([A-Z][A-Z0-9_]*)=", line))
    }
    declared_environment_names = {
        name
        for row in providers
        if row["location"] == "environment"
        for field in ("required_names", "any_of_names", "optional_names")
        for name in row.get(field, [])
    }
    assert declared_environment_names <= example_names

    openbb_example = json.loads(
        (
            root / "configs/openbb_user_settings.credentials.example.json"
        ).read_text(encoding="utf-8")
    )["credentials"]
    declared_openbb_fields = {
        row["openbb_field"]
        for row in providers
        if row.get("openbb_field")
    }
    assert declared_openbb_fields == set(openbb_example)
    assert declared_openbb_fields == set(OPENBB_ENV_TO_CREDENTIAL_FIELD.values())


def test_openbb_env_bridge_applies_only_configured_mapped_fields(tmp_path: Path) -> None:
    credentials = SimpleNamespace(fred_api_key="legacy", bls_api_key="legacy-bls")
    obb = SimpleNamespace(user=SimpleNamespace(credentials=credentials))

    applied = apply_openbb_environment_credentials(
        obb,
        env_file=tmp_path / "missing.env",
        environ={"FRED_API_KEY": "canonical", "BLS_API_KEY": ""},
    )

    assert applied == {"fred_api_key"}
    assert credentials.fred_api_key == "canonical"
    assert credentials.bls_api_key == "legacy-bls"


def test_openbb_legacy_migration_preserves_existing_env_and_source(
    tmp_path: Path,
) -> None:
    env_file = tmp_path / ".env"
    env_file.write_text(
        "FRED_API_KEY=existing-canonical\nBLS_API_KEY=\n",
        encoding="utf-8",
    )
    settings = tmp_path / "user_settings.json"
    source_payload = {
        "credentials": {
            "fred_api_key": "legacy-fred",
            "bls_api_key": "legacy-bls",
            "eia_api_key": "legacy-eia",
        }
    }
    settings.write_text(json.dumps(source_payload), encoding="utf-8")

    receipt = migrate_credentials(
        env_file=env_file,
        openbb_settings=settings,
        dry_run=False,
    )

    migrated = env_file.read_text(encoding="utf-8")
    assert "FRED_API_KEY=existing-canonical" in migrated
    assert "legacy-fred" not in migrated
    assert "BLS_API_KEY=legacy-bls" in migrated
    assert "EIA_API_KEY=legacy-eia" in migrated
    assert "BENZINGA_API_KEY=" in migrated
    assert receipt["secret_values_included"] is False
    assert receipt["legacy_values_deleted"] is False
    assert receipt["already_configured_names"] == ["FRED_API_KEY"]
    assert "BENZINGA_API_KEY" in receipt["reserved_names"]
    assert json.loads(settings.read_text(encoding="utf-8")) == source_payload
    assert oct(os.stat(env_file).st_mode & 0o777) == "0o600"


def test_any_of_credential_slot_is_configured_without_exposing_value(
    tmp_path: Path,
) -> None:
    registry = tmp_path / "registry.json"
    registry.write_text(
        json.dumps(
            {
                "secret_values_permitted": False,
                "providers": [
                    {
                        "id": "identity",
                        "provider": "Identity",
                        "location": "environment",
                        "any_of_names": ["IDENTITY_HEADER", "CONTACT_EMAIL"],
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    env_file = tmp_path / ".env"
    env_file.write_text("CONTACT_EMAIL=secret@example.com\n", encoding="utf-8")
    item = audit._read_registry(registry)[0]
    presence = audit._parse_env_presence(env_file)
    row = audit._presence_row(
        item,
        configured=lambda name: bool(presence.get(name)),
        source="environment",
    )

    assert row["state"] == "configured"
    assert row["configured_count"] == 1
    assert row["required_count"] == 1
    assert "secret@example.com" not in json.dumps(row)
