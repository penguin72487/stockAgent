from __future__ import annotations

import argparse
import io
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import re
import stat
import subprocess
import sys
from typing import Any, Callable


DEFAULT_REGISTRY = Path("configs/data_api_credentials.json")
_ENV_NAME = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_CREDENTIAL_NAME = re.compile(
    r"(?:^|_)(?:API_KEY(?:_ID)?|API_TOKEN|SECRET(?:_KEY)?|TOKEN|PASSWORD|"
    r"PASSCODE|SESSION_ID|CLIENT_ID|CLIENT_SECRET|PRIVATE_KEY|ACCESS_KEY(?:_ID)?)$",
    re.IGNORECASE,
)
_PLACEHOLDERS = {
    "none", "null", "nil", "undefined", "changeme", "change_me", "change-me",
    "replace_me", "replace-me", "placeholder", "todo", "tbd", "n/a", "...",
}
_FINLAB_NAMES = ("FINLAB_REFRESH_TOKEN", "FINLAB_SESSION_ID", "FINLAB_API_KEY")

# get_session is the SDK's local env/file/decrypt reader. Never call login,
# get_id_token, get_status or get_data_status here: those can consume network.
# Isolate it so SDK logging/errors cannot leak tokens into our public receipt.
_FINLAB_SESSION_PROBE = r'''
import contextlib, importlib, json, os, socket

def blocked(*args, **kwargs):
    raise RuntimeError("network_disabled")

socket.socket.connect = blocked
socket.socket.connect_ex = blocked
socket.create_connection = blocked
socket.getaddrinfo = blocked
result = {"state": "unknown", "reason": "sdk_unavailable", "sdk_available": False}
with open(os.devnull, "w") as sink, contextlib.redirect_stdout(sink), contextlib.redirect_stderr(sink):
    try:
        auth = importlib.import_module("finlab.auth")
    except ImportError:
        pass
    except Exception:
        result["reason"] = "sdk_import_failed"
        result["sdk_available"] = None
    else:
        result["sdk_available"] = True
        result["reason"] = "session_unreadable"
        try:
            session = auth.get_session()
            if session is None:
                exists = os.path.exists(auth.CREDENTIALS_FILE)
                result.update(state="unknown" if exists else "missing",
                              reason="session_unusable" if exists else "session_absent")
            elif isinstance(session, dict):
                fields = {name: isinstance(session.get(name), str) and bool(session[name].strip())
                          for name in ("refresh_token", "session_id", "api_key")}
                count = sum(fields.values())
                result.update(state="configured" if count == 3 else "partial" if count else "unknown",
                              reason="session_complete" if count == 3 else "session_incomplete",
                              field_presence=fields)
        except Exception:
            pass
print(json.dumps(result))
'''


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Publish a non-secret credential presence and file-permission receipt."
    )
    parser.add_argument("--env-file", type=Path, default=Path(".env"))
    parser.add_argument("--env-example", type=Path, default=Path(".env.example"))
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


def _value_present(value: Any) -> bool:
    """Literal presence only; never resolve variables, shell code or templates."""
    if not isinstance(value, str):
        return False
    literal = value.strip()
    lowered = literal.casefold()
    if not literal or lowered in _PLACEHOLDERS:
        return False
    if (re.search(r"\$\{|\$\(|`|\{\{", literal)
            or re.fullmatch(r"\$[A-Za-z_][A-Za-z0-9_]*", literal)
            or re.fullmatch(r"<[^>]+>", literal)
            or re.fullmatch(r"[xX*]+", literal)):
        return False
    return not bool(re.fullmatch(
        r"(?:your|replace|insert|enter|put|paste)[_ -]+.*(?:key|token|secret|password|id|here).*",
        lowered,
    ))


def _parse_env_inventory(path: Path) -> dict[str, Any]:
    """Return names/booleans/line numbers only, including dotenv parse failures.

    Reuse the runtime's dotenv lexer, not dotenv_values/load_dotenv: the lexer
    accepts export/quotes/comments/multiline values without any interpolation,
    mutation of os.environ or warnings containing input text.
    """
    result: dict[str, Any] = {
        "state": "missing", "presence": {}, "duplicate_names": [],
        "invalid_line_numbers": [],
    }
    try:
        if not path.is_file():
            return result
        from dotenv.parser import parse_stream
        content = path.read_text(encoding="utf-8-sig")
    except (OSError, UnicodeError, ImportError):
        result["state"] = "unknown"
        return result
    lines: dict[str, list[int]] = {}
    result["state"] = "readable"
    for binding in parse_stream(io.StringIO(content)):
        # Original.line can start on preceding blank lines; expose the actual
        # assignment's line number, never Original.string or a parse exception.
        line_number = binding.original.line
        line_number += len(re.findall(r"\r\n|\r|\n", re.match(r"\s*", binding.original.string)[0]))
        if binding.error or (binding.key and not _ENV_NAME.fullmatch(binding.key)):
            result["invalid_line_numbers"].append(line_number)
            continue
        if binding.key is None:
            continue
        lines.setdefault(binding.key, []).append(line_number)
        empty_comment = re.match(
            rf"\s*(?:export\s+)?(?:{re.escape(binding.key)}|'{re.escape(binding.key)}')\s*=\s*#",
            binding.original.string,
        )
        result["presence"][binding.key] = not empty_comment and _value_present(binding.value)
    result["duplicate_names"] = [
        {"name": name, "lines": numbers}
        for name, numbers in sorted(lines.items()) if len(numbers) > 1
    ]
    return result


def _parse_env_presence(path: Path) -> dict[str, bool]:
    """Backward-compatible boolean-only projection for existing callers."""
    return _parse_env_inventory(path)["presence"]


def _effective_presence(env_presence: dict[str, bool], name: str) -> bool:
    # Match load_dotenv(override=False): even an empty process variable masks
    # the file. Presence is not an attempt to execute an interpolated .env.
    return _value_present(os.environ[name]) if name in os.environ else bool(env_presence.get(name))


def _environment_inventory(
    environment: dict[str, Any], example: dict[str, Any], registry: list[dict[str, Any]],
) -> dict[str, Any]:
    env_names, example_names = set(environment["presence"]), set(example["presence"])
    declared = {
        name for item in registry if item["location"] == "environment"
        for field in ("required_names", "any_of_names", "optional_names")
        for name in item.get(field, [])
    }
    candidates = {name for name in env_names | example_names if _CREDENTIAL_NAME.search(name)}
    return {
        "scope": "env_file_and_example_names; registered_names_include_process_environment",
        "classification": "registered_names_then_credential_suffix_candidates",
        "environment_parse_state": environment["state"],
        "example_parse_state": example["state"],
        "environment_names": sorted(env_names), "example_names": sorted(example_names),
        "environment_only_names": sorted(env_names - example_names),
        "example_only_names": sorted(example_names - env_names),
        "declared_names_missing_example": sorted(declared - example_names),
        "unregistered_credential_names": sorted(candidates - declared),
        "unregistered_credential_slots": [
            {"name": name, "present": _effective_presence(environment["presence"], name),
             "env_file_declared": name in env_names, "example_declared": name in example_names,
             "process_environment_declared": name in os.environ}
            for name in sorted(candidates - declared)
        ],
        "noncredential_configuration_names": sorted((env_names | example_names) - declared - candidates),
        "duplicate_names": {"environment": environment["duplicate_names"], "example": example["duplicate_names"]},
        "invalid_line_numbers": {"environment": environment["invalid_line_numbers"], "example": example["invalid_line_numbers"]},
        "source_precedence": "process_environment_then_env_file; no_interpolation",
    }


def _finlab_session_presence() -> dict[str, Any]:
    """Check local SDK session without leaking child output or using the API."""
    child_env = {key: value for key, value in os.environ.items()
                 if key not in {*_FINLAB_NAMES, "FINLAB_API_TOKEN"}}
    unknown = {"state": "unknown", "reason": "probe_failed", "sdk_available": None}
    try:
        child = subprocess.run(
            [sys.executable, "-c", _FINLAB_SESSION_PROBE], env=child_env,
            stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
            text=True, timeout=10, check=False,
        )
        if child.returncode or len(child.stdout) > 4096:
            return unknown
        result = json.loads(child.stdout)
    except (OSError, subprocess.SubprocessError, UnicodeError, json.JSONDecodeError):
        return unknown
    reasons = {"sdk_unavailable", "sdk_import_failed", "session_unreadable", "session_unusable",
               "session_absent", "session_complete", "session_incomplete"}
    if (not isinstance(result, dict) or not isinstance(result.get("reason"), str)
            or result["reason"] not in reasons):
        return unknown
    state = result.get("state")
    sdk_available = result.get("sdk_available")
    fields = result.get("field_presence", {})
    if (not isinstance(state, str) or state not in {"configured", "partial", "missing", "unknown"}
            or sdk_available is not None and type(sdk_available) is not bool
            or not isinstance(fields, dict)
            or any(type(fields.get(name, False)) is not bool for name in ("refresh_token", "session_id", "api_key"))):
        return unknown
    safe_fields = {name: fields.get(name, False) for name in ("refresh_token", "session_id", "api_key")}
    count = sum(safe_fields.values())
    if state == "configured" and (count != 3 or sdk_available is not True):
        return unknown
    return {"state": state, "reason": result["reason"], "sdk_available": sdk_available,
            "field_presence": safe_fields}


def _apply_finlab_presence(row: dict[str, Any], env_presence: dict[str, bool]) -> None:
    session = _finlab_session_presence()
    row.update(environment_state=row["state"], environment_configured_count=row["configured_count"],
               sdk_session=session, credential_mode="unavailable")
    legacy = _effective_presence(env_presence, "FINLAB_API_TOKEN")
    row["legacy_fallback_configured"] = legacy
    if row["state"] == "configured":
        row["credential_mode"] = "environment_session"
    elif session["state"] == "configured":
        row.update(state="configured", configured_count=3, source="finlab_sdk_session",
                   credential_mode="local_sdk_session")
    elif legacy:
        row.update(state="configured", configured_count=1, required_count=1,
                   credential_mode="legacy_environment_token")
    elif session["state"] == "unknown":
        row["state"] = "unknown"
    elif session["state"] == "partial" and row["state"] == "missing":
        row.update(state="partial", configured_count=sum(session["field_presence"].values()),
                   source="finlab_sdk_session")


def _safe_mode(path: Path) -> tuple[str | None, bool | None]:
    try:
        if not path.exists():
            return None, None
        mode = stat.S_IMODE(path.stat().st_mode)
    except OSError:
        return None, None
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
            configured=lambda name, values=credentials: _value_present(values.get(name)),
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
        if not isinstance(name, str) or not _ENV_NAME.fullmatch(name) or name in declared_names or not any(
            token in lowered for token in ("key", "token", "secret", "password")
        ):
            continue
        rows.append(
            {
                "id": f"openbb:{name}",
                "catalog_id": f"openbb_unregistered:{name}",
                "provider": f"OpenBB credential: {name}",
                "state": "configured" if _value_present(value) else "missing",
                "required_names": [str(name)],
                "any_of_names": [],
                "optional_names": [],
                "configured_count": 1 if _value_present(value) else 0,
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
    environment = _parse_env_inventory(args.env_file)
    example = _parse_env_inventory(args.env_example)
    env_presence = environment["presence"]
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
            configured=lambda name: _effective_presence(env_presence, name),
            source="environment",
        )
        for item in environment_rows
    ]
    for row, item in zip(rows, environment_rows, strict=True):
        if environment["state"] == "unknown" and row["state"] != "configured":
            row["state"] = "unknown"
        if item.get("local_session") == "finlab_sdk":
            _apply_finlab_presence(row, env_presence)
        openbb_field = str(item.get("openbb_field", "")).strip()
        if openbb_field:
            row["legacy_fallback_configured"] = _value_present(
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
        "validation_scope": "local_presence_only; no_authentication_or_entitlement_check",
        "environment_inventory": _environment_inventory(environment, example, registry_rows),
        "files": {
            "registry": {
                "path": str(args.registry),
                "exists": args.registry.is_file(),
            },
            "environment": {"exists": args.env_file.is_file(), "mode": env_mode, "owner_only": env_private},
            "environment_example": {"exists": args.env_example.is_file()},
            "openbb_settings": {
                "exists": args.openbb_settings.is_file(),
                "mode": openbb_mode,
                "owner_only": openbb_private,
                "role": "legacy_fallback",
                "configured_mapped_count": sum(
                    _value_present(legacy_credentials.get(name))
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
        f"unknown={state_counts.get('unknown', 0)} "
        "secret_values_included=false"
    )


if __name__ == "__main__":
    main()
