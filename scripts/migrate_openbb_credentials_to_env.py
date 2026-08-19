from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import re
import stat
import sys
from typing import Any


_REPO_ROOT = Path(__file__).resolve().parents[1]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from downloader.openbb_credentials import OPENBB_ENV_TO_CREDENTIAL_FIELD


_ASSIGNMENT = re.compile(
    r"^(?P<prefix>\s*(?:export\s+)?)(?P<name>[A-Za-z_][A-Za-z0-9_]*)"
    r"(?P<separator>\s*=\s*)(?P<value>.*)$"
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Copy configured OpenBB user-settings credentials into the repository "
            ".env without printing or deleting secret values."
        )
    )
    parser.add_argument("--env-file", type=Path, default=Path(".env"))
    parser.add_argument(
        "--openbb-settings",
        type=Path,
        default=Path.home() / ".openbb_platform/user_settings.json",
    )
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


def _read_legacy_credentials(path: Path) -> dict[str, str]:
    try:
        payload: Any = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise SystemExit(f"OpenBB settings are unreadable: {path}: {exc}") from exc
    credentials = payload.get("credentials") if isinstance(payload, dict) else None
    if not isinstance(credentials, dict):
        raise SystemExit(f"OpenBB settings have no credentials object: {path}")
    return {
        str(name): str(value).strip()
        for name, value in credentials.items()
        if value is not None and str(value).strip()
    }


def _dotenv_value_is_configured(raw_value: str) -> bool:
    value = raw_value.strip()
    if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
        value = value[1:-1]
    return bool(value.strip())


def _quote_dotenv(value: str, *, field_name: str) -> str:
    if "\n" in value or "\r" in value or "\x00" in value:
        raise SystemExit(f"credential field {field_name!r} is not a single-line value")
    if re.fullmatch(r"[A-Za-z0-9_./:+-]+", value):
        return value
    if "'" not in value:
        return f"'{value}'"
    raise SystemExit(
        f"credential field {field_name!r} contains an unsupported quote character"
    )


def migrate_credentials(
    *,
    env_file: Path,
    openbb_settings: Path,
    dry_run: bool,
) -> dict[str, Any]:
    legacy = _read_legacy_credentials(openbb_settings)
    original_text = env_file.read_text(encoding="utf-8") if env_file.is_file() else ""
    lines = original_text.splitlines()
    target_names = set(OPENBB_ENV_TO_CREDENTIAL_FIELD)
    locations: dict[str, list[int]] = {name: [] for name in target_names}
    matches: dict[int, re.Match[str]] = {}
    for index, line in enumerate(lines):
        match = _ASSIGNMENT.match(line)
        if match and match.group("name") in target_names:
            name = match.group("name")
            locations[name].append(index)
            matches[index] = match
    duplicated = sorted(name for name, indexes in locations.items() if len(indexes) > 1)
    if duplicated:
        raise SystemExit(
            "refusing ambiguous dotenv migration; duplicate fields: "
            + ", ".join(duplicated)
        )

    migrated: list[str] = []
    reserved: list[str] = []
    already_configured: list[str] = []
    unavailable: list[str] = []
    append_lines: list[str] = []
    for env_name, openbb_field in OPENBB_ENV_TO_CREDENTIAL_FIELD.items():
        indexes = locations[env_name]
        if indexes and _dotenv_value_is_configured(matches[indexes[0]].group("value")):
            already_configured.append(env_name)
            continue
        value = legacy.get(openbb_field, "")
        if not value:
            unavailable.append(env_name)
            if not indexes:
                append_lines.append(f"{env_name}=")
                reserved.append(env_name)
            continue
        rendered = _quote_dotenv(value, field_name=openbb_field)
        if indexes:
            index = indexes[0]
            match = matches[index]
            lines[index] = (
                f"{match.group('prefix')}{env_name}{match.group('separator')}{rendered}"
            )
        else:
            append_lines.append(f"{env_name}={rendered}")
        migrated.append(env_name)

    if append_lines:
        if lines and lines[-1].strip():
            lines.append("")
        lines.append("# OpenBB provider credentials (canonical; migrated from user settings)")
        lines.extend(append_lines)
    changed = bool(migrated or reserved)
    if changed and not dry_run:
        env_file.parent.mkdir(parents=True, exist_ok=True)
        temporary = env_file.with_name(f".{env_file.name}.{os.getpid()}.tmp")
        try:
            temporary.write_text("\n".join(lines) + "\n", encoding="utf-8")
            os.chmod(temporary, stat.S_IRUSR | stat.S_IWUSR)
            os.replace(temporary, env_file)
        finally:
            temporary.unlink(missing_ok=True)
    elif env_file.exists() and not dry_run:
        os.chmod(env_file, stat.S_IRUSR | stat.S_IWUSR)

    return {
        "schema_version": 1,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "secret_values_included": False,
        "dry_run": dry_run,
        "changed": changed,
        "migrated_names": sorted(migrated),
        "reserved_names": sorted(reserved),
        "already_configured_names": sorted(already_configured),
        "unavailable_names": sorted(unavailable),
        "legacy_values_deleted": False,
    }


def main() -> None:
    args = parse_args()
    receipt = migrate_credentials(
        env_file=args.env_file,
        openbb_settings=args.openbb_settings,
        dry_run=args.dry_run,
    )
    print(json.dumps(receipt, ensure_ascii=False, sort_keys=True))


if __name__ == "__main__":
    main()
