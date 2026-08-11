#!/usr/bin/env python3
"""Audit the shared data and distinct model artifacts for four TW day trades."""

from __future__ import annotations

import argparse
from datetime import datetime
import json
import os
from pathlib import Path
import subprocess
import sys
import uuid
from zoneinfo import ZoneInfo


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from stockagent.live.market_config import load_market_configs  # noqa: E402
from stockagent.live.market_status import runtime_status  # noqa: E402


TAIPEI = ZoneInfo("Asia/Taipei")
MARKETS = (
    "tw_day_trade_1m",
    "tw_day_trade",
    "tw_day_trade_multi_basis",
    "tw_day_trade_100m",
)


def _read_json(path: Path) -> dict[str, object] | None:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None
    return value if isinstance(value, dict) else None


def _service_state(name: str) -> dict[str, str]:
    def query(command: str) -> str:
        result = subprocess.run(
            ["systemctl", command, name],
            check=False,
            capture_output=True,
            text=True,
        )
        return (result.stdout or result.stderr).strip()

    return {"active": query("is-active"), "enabled": query("is-enabled")}


def _atomic_text(path: Path, value: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + f".tmp.{uuid.uuid4().hex}")
    temporary.write_text(value, encoding="utf-8")
    os.replace(temporary, path)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("artifacts/live/tw_day_trade_readiness"),
    )
    parser.add_argument("--strict", action="store_true")
    args = parser.parse_args()

    configs = load_market_configs(REPO_ROOT / "services/discord_bot/markets")
    missing_configs = sorted(set(MARKETS).difference(configs))
    if missing_configs:
        raise RuntimeError(f"missing day-trade market configs: {missing_configs}")
    statuses = [runtime_status(configs[name], root=REPO_ROOT) for name in MARKETS]
    shared_commands = {item.cfg.pre_signal_command for item in statuses}
    active_data = REPO_ROOT / "data_tw_public"
    active_target = (
        str(active_data.resolve(strict=True)) if active_data.is_symlink() else None
    )
    summary = _read_json(active_data / "download_summary.json") or {}
    pin = _read_json(Path("/srv/stockagent-snapshots/tw-public.pin.json")) or {}
    rows: list[dict[str, object]] = []
    for item in statuses:
        weights = (
            (REPO_ROOT / item.cfg.weights_path)
            if item.cfg.weights_path
            else None
        )
        row = {
            "market": item.cfg.market,
            "label": item.cfg.label,
            "enabled": item.enabled,
            "runtime_status": item.status,
            "data_ready": item.data.fresh,
            "last_data_date": item.data.last_data_date,
            "expected_latest_date": item.data.expected_latest_date,
            "data_reason": item.data.reason,
            "checkpoint_ready": item.checkpoint is not None,
            "checkpoint_path": (
                str(item.checkpoint.path)
                if item.checkpoint is not None
                else item.cfg.checkpoint_path
            ),
            "weights_history_ready": bool(weights and weights.is_file()),
            "weights_path": str(weights) if weights is not None else None,
            "execution_ready": item.status == "ready",
        }
        rows.append(row)

    pin_manifest = pin.get("manifest")
    payload: dict[str, object] = {
        "schema_version": 1,
        "generated_at_taipei": datetime.now(TAIPEI).isoformat(),
        "shared_data_contract": {
            "one_refresh_command_for_four_modes": len(shared_commands) == 1,
            "refresh_command": list(next(iter(shared_commands)))
            if shared_commands
            else [],
            "active_path_is_symlink": active_data.is_symlink(),
            "active_materialized_path": active_target,
            "download_end_date": summary.get("end_date"),
            "coverage_complete": summary.get("coverage_complete"),
            "daily_close_ready": summary.get("daily_close_ready"),
            "pin_snapshot_id": pin_manifest.get("snapshot_id")
            if isinstance(pin_manifest, dict)
            else None,
        },
        "services": {
            "discord_bot": _service_state("stockagent-discord-bot.service"),
            "taifex_simulation": _service_state(
                "stockagent-shioaji-taifex-bidask.service"
            ),
            "syncthing": _service_state("syncthing@root.service"),
        },
        "markets": rows,
        "counts": {
            "markets": len(rows),
            "data_ready": sum(bool(row["data_ready"]) for row in rows),
            "checkpoint_ready": sum(
                bool(row["checkpoint_ready"]) for row in rows
            ),
            "execution_ready": sum(bool(row["execution_ready"]) for row in rows),
        },
    }
    output_dir = (
        args.output_dir
        if args.output_dir.is_absolute()
        else REPO_ROOT / args.output_dir
    )
    output_dir.mkdir(parents=True, exist_ok=True)
    _atomic_text(
        output_dir / "readiness.json",
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
    )

    lines = [
        "# 台股四個當沖模式 readiness",
        "",
        f"更新時間：{payload['generated_at_taipei']}",
        "",
        "四個模式共用同一份已驗證的台股資料快照；模型 checkpoint 與資金契約各自獨立。",
        "",
        "| 模式 | 資料 | Checkpoint | 權重歷史 | 可執行 | 資料日期 / 期望日期 |",
        "| --- | ---: | ---: | ---: | ---: | --- |",
    ]
    for row in rows:
        lines.append(
            "| {label} | {data} | {checkpoint} | {weights} | {execution} | "
            "{last} / {expected} |".format(
                label=row["label"],
                data="✅" if row["data_ready"] else "❌",
                checkpoint="✅" if row["checkpoint_ready"] else "❌",
                weights="✅" if row["weights_history_ready"] else "❌",
                execution="✅" if row["execution_ready"] else "❌",
                last=row["last_data_date"] or "—",
                expected=row["expected_latest_date"] or "—",
            )
        )
    counts = payload["counts"]
    assert isinstance(counts, dict)
    services = payload["services"]
    assert isinstance(services, dict)
    lines.extend(
        [
            "",
            "## 結論",
            "",
            f"- 資料 ready：{counts['data_ready']}/{counts['markets']}。",
            f"- Checkpoint ready：{counts['checkpoint_ready']}/{counts['markets']}。",
            f"- 端到端可執行：{counts['execution_ready']}/{counts['markets']}。",
            "- 缺 checkpoint 時維持 fail-closed，不會拿別的模型冒充該資金模式。",
            "",
            "## 常駐服務",
            "",
            f"- Discord bot：{services['discord_bot']}",
            f"- TAIFEX 模擬：{services['taifex_simulation']}",
            f"- Syncthing：{services['syncthing']}",
            "",
        ]
    )
    _atomic_text(output_dir / "readiness.md", "\n".join(lines))
    print(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True))
    return 1 if args.strict and int(counts["execution_ready"]) != len(rows) else 0


if __name__ == "__main__":
    raise SystemExit(main())
