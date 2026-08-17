from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import re
import stat
from typing import Any


ENV_PROVIDERS: tuple[dict[str, Any], ...] = (
    {"id": "shioaji", "provider": "SinoPac Shioaji", "required": ("SHIOAJI_API_KEY", "SHIOAJI_SECRET_KEY")},
    {"id": "alpaca", "provider": "Alpaca", "required": ("APCA_API_KEY_ID", "APCA_API_SECRET_KEY")},
    {"id": "alpha_vantage", "provider": "Alpha Vantage", "required": ("ALPHAVANTAGE_API_KEY",)},
    {"id": "finnhub", "provider": "Finnhub", "required": ("FINNHUB_API_KEY",)},
    {"id": "nasdaq_data_link", "provider": "Nasdaq Data Link", "required": ("NASDAQ_DATA_LINK_API_KEY",)},
    {"id": "noaa_cdo", "provider": "NOAA CDO", "required": ("NOAA_CDO_TOKEN",)},
    {"id": "cwa", "provider": "臺灣中央氣象署 CWA", "required": ("CWA_API_KEY",)},
    {"id": "moenv", "provider": "環境部 MOENV", "required": ("MOENV_API_KEY",)},
    {"id": "etherscan", "provider": "Etherscan", "required": ("ETHERSCAN_API_KEY",)},
    {"id": "dune", "provider": "Dune", "required": ("DUNE_API_KEY",)},
    {"id": "coingecko", "provider": "CoinGecko Demo", "required": ("COINGECKO_DEMO_API_KEY",)},
    {"id": "coinglass", "provider": "CoinGlass", "required": ("COINGLASS_API_KEY",)},
    {"id": "coinmarketcap", "provider": "CoinMarketCap", "required": ("COINMARKETCAP_API_KEY",)},
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Publish a non-secret credential presence and file-permission receipt."
    )
    parser.add_argument("--env-file", type=Path, default=Path(".env"))
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


def _openbb_rows(path: Path) -> list[dict[str, Any]]:
    if not path.is_file():
        return []
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError):
        return []
    credentials = payload.get("credentials") if isinstance(payload, dict) else None
    if not isinstance(credentials, dict):
        credentials = payload if isinstance(payload, dict) else {}
    rows = []
    for name, value in sorted(credentials.items()):
        lowered = str(name).lower()
        if not any(token in lowered for token in ("key", "token", "secret", "password")):
            continue
        rows.append(
            {
                "id": f"openbb:{name}",
                "provider": f"OpenBB credential: {name}",
                "state": "configured" if bool(value) else "missing",
                "required_names": [str(name)],
                "configured_count": 1 if bool(value) else 0,
                "required_count": 1,
                "source": "openbb_settings",
            }
        )
    return rows


def main() -> None:
    args = parse_args()
    env_presence = _parse_env_presence(args.env_file)
    rows: list[dict[str, Any]] = []
    for item in ENV_PROVIDERS:
        names = tuple(str(name) for name in item["required"])
        configured = sum(bool(env_presence.get(name) or os.environ.get(name, "").strip()) for name in names)
        state = "configured" if configured == len(names) else "partial" if configured else "missing"
        rows.append(
            {
                "id": item["id"],
                "provider": item["provider"],
                "state": state,
                "required_names": list(names),
                "configured_count": configured,
                "required_count": len(names),
                "source": "environment",
            }
        )
    rows.extend(_openbb_rows(args.openbb_settings))
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
            "environment": {"exists": args.env_file.is_file(), "mode": env_mode, "owner_only": env_private},
            "openbb_settings": {"exists": args.openbb_settings.is_file(), "mode": openbb_mode, "owner_only": openbb_private},
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
