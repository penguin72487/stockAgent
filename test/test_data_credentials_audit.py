from __future__ import annotations

import json
import os
from pathlib import Path
import sys

from scripts import audit_data_credentials as audit


def test_credential_audit_publishes_presence_without_secret_values(
    tmp_path: Path, monkeypatch
) -> None:
    env_file = tmp_path / ".env"
    env_file.write_text(
        "SHIOAJI_API_KEY=very-secret-api-key\n"
        "SHIOAJI_SECRET_KEY=very-secret-secret-key\n"
        "FINNHUB_API_KEY=another-secret\n",
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
    assert next(row for row in payload["providers"] if row["id"] == "shioaji")[
        "state"
    ] == "configured"
    assert next(
        row for row in payload["providers"] if row["id"] == "openbb:fred_api_key"
    )["state"] == "configured"
    assert oct(os.stat(output).st_mode & 0o777) == "0o600"
