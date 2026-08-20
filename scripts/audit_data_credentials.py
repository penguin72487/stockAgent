from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import re
import stat
from typing import Any, Callable


DEFAULT_REGISTRY = Path("configs/data_api_credentials.json")
_ENV_NAME = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Publish a non-secret credential presence and file-permission receipt."
    )
    parser.add_argument("--env-file", type=Path, default=Path(".env"))
    parser.add_argument("--registry", type=Path, default=DEFAULT_REGISTRY)
    parser.add_argument(
        "--openbb-settings",
        type=Path,
        default=Path.home() / ".openbb_platform/user_settings.json",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("artifacts/data_credentials/status.json"),
    )
    return parser.parse_args()


def _parse_env_presence(path: Path) -> dict[str, bool]:
    values: dict[str, bool] = {}
    if not path.is_file():
        return values
    pattern = re.compile(r"^([A-Za-z_][A-Za-z0-9_]*)\s*=\s*(.*)$")
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        match = pattern.match(line)
        if not match:
            continue
        raw_value = match.group(2).strip()
        if len(raw_value) >= 2 and raw_value[0] == raw_value[-1] and raw_value[0] in {"'", '"'}:
            raw_value = raw_value[1:-1]
        values[match.group(1)] = bool(raw_value.strip())
    return values


def _safe_mode(path: Path) -> tuple[str | None, bool | None]:
    if not path.exists():
        return None, None
    mode = stat.S_IMODE(path.stat().st_mode)
    return f"{mode:03o}", (mode & 0o077) == 0


def _read_registry(path: Path) -> list[dict[str, Any]]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise SystemExit(f"credential registry is unreadable: {path}: {exc}") from exc
    if not isinstance(payload, dict) or payload.get("secret_values_permitted") is not False:
        raise SystemExit(
            f"credential registry must explicitly forbid secret values: {path}"
        )
    providers = payload.get("providers")
    if not isinstance(providers, list):
        raise SystemExit(f"credential registry providers must be a list: {path}")
    rows: list[dict[str, Any]] = []
    seen_ids: set[str] = set()
    for raw in providers:
        if not isinstance(raw, dict):
            raise SystemExit(f"credential registry provider must be an object: {path}")
        item = dict(raw)
        provider_id = str(item.get("id", "")).strip()
        location = str(item.get("location", "")).strip()
        required_names = _string_names(item.get("required_names"))
        any_of_names = _string_names(item.get("any_of_names"))
        optional_names = _string_names(item.get("optional_names"))
        if not provider_id or provider_id in seen_ids:
            raise SystemExit(
                f"credential registry provider id is empty or duplicated: {provider_id!r}"
            )
        if location not in {"environment", "openbb_settings"}:
            raise SystemExit(
                f"credential registry provider {provider_id!r} has invalid location"
            )
        if not required_names and not any_of_names:
            raise SystemExit(
                f"credential registry provider {provider_id!r} has no credential names"
            )
        for name in (*required_names, *any_of_names, *optional_names):
            if not _ENV_NAME.fullmatch(name):
                raise SystemExit(
                    f"credential registry provider {provider_id!r} has invalid name {name!r}"
                )
        item["id"] = provider_id
        item["location"] = location
        item["required_names"] = list(required_names)
        item["any_of_names"] = list(any_of_names)
        item["optional_names"] = list(optional_names)
        seen_ids.add(provider_id)
        rows.append(item)
    return rows


def _string_names(value: Any) -> tuple[str, ...]:
    if not isinstance(value, (list, tuple)):
        return ()
    return tuple(str(name).strip() for name in value if str(name).strip())


def _presence_row(
    item: dict[str, Any],
    *,
    configured: Callable[[str], bool],
    source: str,
) -> dict[str, Any]:
    required_names = _string_names(item.get("required_names"))
    any_of_names = _string_names(item.get("any_of_names"))
    optional_names = _string_names(item.get("optional_names"))
    configured_required = sum(configured(name) for name in required_names)
    configured_any = sum(configured(name) for name in any_of_names)
    required_count = len(required_names) + (1 if any_of_names else 0)
    configured_count = configured_required + (1 if configured_any else 0)
    state = (
        "configured"
        if configured_count == required_count
        else "partial"
        if configured_count
        else "missing"
    )
    openbb_field = str(item.get("openbb_field", "")).strip()
    provider_id = str(item["id"])
    if openbb_field:
        provider_id = f"openbb:{openbb_field}"
    elif source == "openbb_settings" and len(required_names) == 1 and not any_of_names:
        provider_id = f"openbb:{required_names[0]}"
    return {
        "id": provider_id,
        "catalog_id": str(item["id"]),
        "provider": str(item.get("provider") or item["id"]),
        "state": state,
        "required_names": list(required_names),
        "any_of_names": list(any_of_names),
        "optional_names": list(optional_names),
        "configured_count": configured_count,
        "required_count": required_count,
        "source": source,
        "storage_location": str(item["location"]),
        "openbb_field": openbb_field or None,
        "requirement": str(item.get("requirement", "unspecified")),
        "registration_url": str(item.get("registration_url", "")),
        "notes": str(item.get("notes", "")),
    }


def _openbb_credentials(path: Path) -> dict[str, Any]:
    if not path.is_file():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError):
        return {}
    credentials = payload.get("credentials") if isinstance(payload, dict) else None
    if not isinstance(credentials, dict):
        credentials = payload if isinstance(payload, dict) else {}
    return credentials


def _openbb_rows(
    path: Path,
    declared: list[dict[str, Any]],
    *,
    known_names: set[str] | None = None,
) -> list[dict[str, Any]]:
    credentials = _openbb_credentials(path)
    rows = [
        _presence_row(
            item,
            configured=lambda name, values=credentials: bool(values.get(name)),
            source="openbb_settings",
        )
        for item in declared
    ]
    declared_names = {
        name
        for item in declared
        for name in (
            *_string_names(item.get("required_names")),
            *_string_names(item.get("any_of_names")),
            *_string_names(item.get("optional_names")),
        )
    }
    declared_names.update(known_names or set())
    for name, value in sorted(credentials.items()):
        lowered = str(name).lower()
        if name in declared_names or not any(
            token in lowered for token in ("key", "token", "secret", "password")
        ):
            continue
        rows.append(
            {
                "id": f"openbb:{name}",
                "catalog_id": f"openbb_unregistered:{name}",
                "provider": f"OpenBB credential: {name}",
                "state": "configured" if bool(value) else "missing",
                "required_names": [str(name)],
                "any_of_names": [],
                "optional_names": [],
                "configured_count": 1 if bool(value) else 0,
                "required_count": 1,
                "source": "openbb_settings",
                "storage_location": "openbb_settings",
                "requirement": "unregistered",
                "registration_url": "",
                "notes": "OpenBB exposes this field but it is not yet declared in the project registry.",
            }
        )
    return rows


def main() -> None:
    args = parse_args()
    registry_rows = _read_registry(args.registry)
    env_presence = _parse_env_presence(args.env_file)
    environment_rows = [
        item for item in registry_rows if item["location"] == "environment"
    ]
    openbb_rows = [
        item for item in registry_rows if item["location"] == "openbb_settings"
    ]
    legacy_credentials = _openbb_credentials(args.openbb_settings)
    rows = [
        _presence_row(
            item,
            configured=lambda name: bool(
                env_presence.get(name) or os.environ.get(name, "").strip()
            ),
            source="environment",
        )
        for item in environment_rows
    ]
    for row, item in zip(rows, environment_rows, strict=True):
        openbb_field = str(item.get("openbb_field", "")).strip()
        if openbb_field:
            row["legacy_fallback_configured"] = bool(
                legacy_credentials.get(openbb_field)
            )
    canonical_openbb_fields = {
        str(item.get("openbb_field", "")).strip()
        for item in registry_rows
        if str(item.get("openbb_field", "")).strip()
    }
    rows.extend(
        _openbb_rows(
            args.openbb_settings,
            openbb_rows,
            known_names=canonical_openbb_fields,
        )
    )
    env_mode, env_private = _safe_mode(args.env_file)
    openbb_mode, openbb_private = _safe_mode(args.openbb_settings)
    state_counts: dict[str, int] = {}
    for row in rows:
        state = str(row["state"])
        state_counts[state] = state_counts.get(state, 0) + 1
    payload = {
        "schema_version": 1,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "secret_values_included": False,
        "files": {
            "registry": {
                "path": str(args.registry),
                "exists": args.registry.is_file(),
            },
            "environment": {"exists": args.env_file.is_file(), "mode": env_mode, "owner_only": env_private},
            "openbb_settings": {
                "exists": args.openbb_settings.is_file(),
                "mode": openbb_mode,
                "owner_only": openbb_private,
                "role": "legacy_fallback",
                "configured_mapped_count": sum(
                    bool(legacy_credentials.get(name))
                    for name in canonical_openbb_fields
                ),
            },
        },
        "state_counts": dict(sorted(state_counts.items())),
        "providers": rows,
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    temporary = args.output.with_name(f".{args.output.name}.{os.getpid()}.tmp")
    temporary.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    os.chmod(temporary, 0o600)
    os.replace(temporary, args.output)
    print(
        "[credentials] "
        f"providers={len(rows)} configured={state_counts.get('configured', 0)} "
        f"partial={state_counts.get('partial', 0)} missing={state_counts.get('missing', 0)} "
        "secret_values_included=false"
    )


if __name__ == "__main__":
    main()
