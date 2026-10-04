from __future__ import annotations

import json
import os
from pathlib import Path
import re
import sys
from types import SimpleNamespace

import pytest

from downloader.openbb_credentials import (
    OPENBB_ENV_TO_CREDENTIAL_FIELD,
    apply_openbb_environment_credentials,
)
from scripts import audit_data_credentials as audit
from scripts.migrate_openbb_credentials_to_env import migrate_credentials


def test_credential_audit_publishes_presence_without_secret_values(
    tmp_path: Path, monkeypatch
) -> None:
    monkeypatch.setattr(audit, "_finlab_session_presence", lambda: {
        "state": "missing", "reason": "session_absent", "sdk_available": True,
        "field_presence": {"refresh_token": False, "session_id": False, "api_key": False},
    })
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
    assert {row["id"] for row in payload["providers"]} >= {"finmind", "finlab"}
    assert payload["validation_scope"].startswith("local_presence_only")
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


def test_env_parser_uses_dotenv_quoting_without_interpolation(tmp_path, monkeypatch):
    path = tmp_path / ".env"
    monkeypatch.setenv("REAL_TOKEN", "must-not-be-resolved")
    path.write_text(
        "# ignored\n\nexport API_KEY='literal#secret' # comment\n"
        'SECRET_TOKEN="escaped\\\"secret" # private\n'
        "EMPTY_TOKEN= # placeholder\n"
        "HASH_TOKEN=literal#secret\n"
        "COMMENT_TOKEN=literal # private comment\n"
        "REF_TOKEN=${REAL_TOKEN}\n"
        "BARE_TOKEN=$REAL_TOKEN\n"
        "COMMAND_TOKEN=$(touch should-never-exist)\n"
        "PLACEHOLDER_KEY=<YOUR_API_KEY>\n"
        "DUP_TOKEN=first-secret\nDUP_TOKEN=\n"
        "FINLAB_TICK_QUOTA_RESERVE_MB=500\n"
        "BAD NAME=private-invalid-value\n",
        encoding="utf-8",
    )
    before = dict(os.environ)
    inventory = audit._parse_env_inventory(path)
    assert inventory["presence"] == {
        "API_KEY": True, "SECRET_TOKEN": True, "EMPTY_TOKEN": False,
        "HASH_TOKEN": True, "COMMENT_TOKEN": True, "REF_TOKEN": False,
        "BARE_TOKEN": False, "COMMAND_TOKEN": False, "PLACEHOLDER_KEY": False,
        "DUP_TOKEN": False, "FINLAB_TICK_QUOTA_RESERVE_MB": True,
    }
    assert inventory["duplicate_names"] == [{"name": "DUP_TOKEN", "lines": [12, 13]}]
    assert inventory["invalid_line_numbers"] == [15]
    assert dict(os.environ) == before
    assert "private-invalid-value" not in json.dumps(inventory)
    assert "first-secret" not in json.dumps(inventory)
    assert "literal#secret" not in json.dumps(inventory)
    assert not (tmp_path / "should-never-exist").exists()


@pytest.mark.parametrize("literal", [
    "", "  ", "None", "null", "changeme", "YOUR_API_KEY", "your-token-here",
    "replace_with_your_key", "<token>", "${API_KEY}", "$SECRET", "$(printenv)",
    "`printenv`", "{{ token }}", "xxxxxxxx", "********", "...",
])
def test_placeholder_literals_are_not_credentials(literal):
    assert audit._value_present(literal) is False


def test_env_parser_supports_multiline_and_bom(tmp_path):
    path = tmp_path / ".env"
    path.write_text('\ufeffexport CERT_KEY="line1\nline2"\n\nCERT_KEY=\n', encoding="utf-8")
    result = audit._parse_env_inventory(path)
    assert result["presence"] == {"CERT_KEY": False}
    assert result["duplicate_names"] == [{"name": "CERT_KEY", "lines": [1, 4]}]


def test_process_environment_masks_file_even_when_empty(monkeypatch):
    monkeypatch.setenv("FINMIND_TOKEN", "")
    assert audit._effective_presence({"FINMIND_TOKEN": True}, "FINMIND_TOKEN") is False
    monkeypatch.setenv("FINMIND_TOKEN", "YOUR_TOKEN_HERE")
    assert audit._effective_presence({"FINMIND_TOKEN": True}, "FINMIND_TOKEN") is False
    monkeypatch.delenv("FINMIND_TOKEN")
    assert audit._effective_presence({"FINMIND_TOKEN": True}, "FINMIND_TOKEN") is True


def test_env_name_coverage_separates_credentials_from_configuration(tmp_path):
    env_path, example_path = tmp_path / ".env", tmp_path / ".env.example"
    env_path.write_text(
        "FINMIND_TOKEN=private-token\nUNREGISTERED_API_KEY=secret\nAPI_TIMEOUT_SECONDS=30\n"
        "FINLAB_TICK_QUOTA_RESERVE_MB=500\nMAX_TOKENS=100\n",
        encoding="utf-8",
    )
    example_path.write_text("FINMIND_TOKEN=\nEXAMPLE_TOKEN=\n", encoding="utf-8")
    registry = audit._read_registry(Path("configs/data_api_credentials.json"))
    result = audit._environment_inventory(
        audit._parse_env_inventory(env_path), audit._parse_env_inventory(example_path), registry,
    )
    assert result["unregistered_credential_names"] == ["EXAMPLE_TOKEN", "UNREGISTERED_API_KEY"]
    assert result["noncredential_configuration_names"] == [
        "API_TIMEOUT_SECONDS", "FINLAB_TICK_QUOTA_RESERVE_MB", "MAX_TOKENS",
    ]
    assert result["example_only_names"] == ["EXAMPLE_TOKEN"]
    assert "FINLAB_API_KEY" in result["declared_names_missing_example"]
    assert "private-token" not in json.dumps(result)


def _fake_finlab_sdk(tmp_path, monkeypatch, body):
    package = tmp_path / "finlab"
    package.mkdir()
    (package / "__init__.py").write_text("", encoding="utf-8")
    (package / "auth.py").write_text(body, encoding="utf-8")
    monkeypatch.setenv("PYTHONPATH", str(tmp_path))


def test_finlab_local_probe_sanitizes_sdk_and_isolates_environment(tmp_path, monkeypatch, capsys):
    _fake_finlab_sdk(tmp_path, monkeypatch, '''
import os, sys
print("private-import-output", file=sys.stderr)
def get_session():
    assert not any(os.environ.get(name) for name in
                   ("FINLAB_API_KEY", "FINLAB_REFRESH_TOKEN", "FINLAB_SESSION_ID", "FINLAB_API_TOKEN"))
    print("private-session-output")
    return {"refresh_token": "private-refresh", "session_id": "private-id", "api_key": "private-key",
            "extra_secret": "must-never-escape"}
''')
    monkeypatch.setenv("FINLAB_API_KEY", "real-env-must-not-be-changed")
    result = audit._finlab_session_presence()
    assert result["state"] == "configured"
    assert result["sdk_available"] is True
    assert all(result["field_presence"].values())
    assert os.environ["FINLAB_API_KEY"] == "real-env-must-not-be-changed"
    assert "private" not in json.dumps(result)
    assert "must-never-escape" not in json.dumps(result)
    assert capsys.readouterr() == ("", "")


@pytest.mark.parametrize(("body", "state", "reason"), [
    ("raise ImportError('private-import-detail')", "unknown", "sdk_unavailable"),
    ("CREDENTIALS_FILE='/definitely/missing/finlab/session'\ndef get_session(): return None",
     "missing", "session_absent"),
    ("CREDENTIALS_FILE=__file__\ndef get_session(): return None", "unknown", "session_unusable"),
    ("def get_session(): raise ValueError('private-session-detail')", "unknown", "session_unreadable"),
    ("def get_session(): return {'api_key': 'private-partial'}", "partial", "session_incomplete"),
    ("def get_session(): return 'private-unexpected-shape'", "unknown", "session_unreadable"),
    ("import socket\ndef get_session(): socket.create_connection(('example.com',443))",
     "unknown", "session_unreadable"),
])
def test_finlab_probe_handles_missing_corrupt_sdk_and_blocks_network(tmp_path, monkeypatch, body, state, reason):
    _fake_finlab_sdk(tmp_path, monkeypatch, body)
    result = audit._finlab_session_presence()
    assert result["state"] == state
    assert result["reason"] == reason
    assert "private" not in json.dumps(result)


@pytest.mark.parametrize("stdout", [
    "private-token-not-json",
    json.dumps({"state": "configured", "reason": "private-token"}),
    json.dumps({"state": "configured", "reason": "session_complete", "sdk_available": True}),
    json.dumps({"state": "configured", "reason": "session_complete", "sdk_available": True,
                "field_presence": {"api_key": "private-token"}}),
])
def test_finlab_probe_rejects_untrusted_child_shape_without_echo(monkeypatch, stdout):
    monkeypatch.setattr(audit.subprocess, "run", lambda *a, **k: SimpleNamespace(returncode=0, stdout=stdout))
    result = audit._finlab_session_presence()
    assert result == {"state": "unknown", "reason": "probe_failed", "sdk_available": None}


def test_finlab_probe_timeout_never_echoes_exception(monkeypatch):
    def fail(*args, **kwargs):
        raise audit.subprocess.TimeoutExpired("private-command", 10, output="private-token")
    monkeypatch.setattr(audit.subprocess, "run", fail)
    assert audit._finlab_session_presence()["state"] == "unknown"


@pytest.mark.parametrize(("env_count", "sdk_state", "legacy", "state", "mode"), [
    (3, "unknown", False, "configured", "environment_session"),
    (0, "configured", False, "configured", "local_sdk_session"),
    (1, "configured", False, "configured", "local_sdk_session"),
    (0, "unknown", True, "configured", "legacy_environment_token"),
    (0, "unknown", False, "unknown", "unavailable"),
    (0, "missing", False, "missing", "unavailable"),
    (1, "missing", False, "partial", "unavailable"),
    (0, "partial", False, "partial", "unavailable"),
])
def test_finlab_effective_presence_does_not_confuse_missing_env_with_local_session(
    monkeypatch, env_count, sdk_state, legacy, state, mode,
):
    for name in (*audit._FINLAB_NAMES, "FINLAB_API_TOKEN"):
        monkeypatch.delenv(name, raising=False)
    item = next(row for row in audit._read_registry(Path("configs/data_api_credentials.json")) if row["id"] == "finlab")
    presence = {name: index < env_count for index, name in enumerate(audit._FINLAB_NAMES)}
    presence["FINLAB_API_TOKEN"] = legacy
    row = audit._presence_row(item, configured=lambda name: presence.get(name, False), source="environment")
    monkeypatch.setattr(audit, "_finlab_session_presence", lambda: {
        "state": sdk_state, "reason": "test", "sdk_available": True,
        "field_presence": {"api_key": sdk_state in {"configured", "partial"},
                           "session_id": sdk_state == "configured", "refresh_token": sdk_state == "configured"},
    })
    audit._apply_finlab_presence(row, presence)
    assert row["state"] == state
    assert row["credential_mode"] == mode
    assert row["environment_configured_count"] == env_count


def test_unreadable_env_is_unknown_without_exception_details(tmp_path, monkeypatch):
    path = tmp_path / ".env"
    path.write_text("SECRET_TOKEN=private-token", encoding="utf-8")
    def unreadable(*args, **kwargs):
        raise PermissionError("private-error-detail")
    monkeypatch.setattr(Path, "read_text", unreadable)
    result = audit._parse_env_inventory(path)
    assert result["state"] == "unknown"
    assert "private" not in json.dumps(result)
