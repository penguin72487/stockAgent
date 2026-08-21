"""Public-safe, source-backed status for every registered data collection.

The physical registry remains ``configs/data_sync/packed_datasets.json``.  This
module projects that registry together with the existing Shioaji/OpenBB status
builders and receipt-backed dataset manifests.  It intentionally distinguishes
freshness, historical completeness, process activity, and ETA; those concepts
must not be collapsed into one optimistic health flag.
"""

from __future__ import annotations

import csv
from datetime import UTC, date, datetime, time as datetime_time, timedelta
import json
import math
from pathlib import Path
import re
import subprocess
from typing import Any, Final, Iterable, Mapping
from zoneinfo import ZoneInfo

from stockagent.data.taifex_sessions import next_taifex_capture_window
from stockagent.live.openbb_archive_dashboard import build_openbb_public_status
from stockagent.live.shioaji_api_dashboard import build_shioaji_public_status


DATA_MONITOR_SCHEMA_VERSION: Final[int] = 4
TAIPEI: Final[ZoneInfo] = ZoneInfo("Asia/Taipei")

_GROUP_META: Final[dict[str, dict[str, Any]]] = {
    "tw-public": {
        "title": "臺灣官方公開資料",
        "provider": "TWSE / TPEx / MOPS / CBC / TDCC",
        "cadence": "每個交易日收盤後",
        "owner": "不可變快照更新器",
        "window": 72 * 3600,
    },
    "tw-minute-train": {
        "title": "台股一分鐘研究資料",
        "provider": "永豐 Shioaji",
        "cadence": "交易日持續增量",
        "owner": "Shioaji 分鐘資料建置器",
        "window": 72 * 3600,
    },
    "tw-minute-source-cold": {
        "title": "台股一分鐘原始分片",
        "provider": "永豐 Shioaji",
        "cadence": "交易日持續增量",
        "owner": "Shioaji 分鐘回補器",
        "window": 72 * 3600,
    },
    "tw-microstructure-train": {
        "title": "台股微結構研究資料",
        "provider": "永豐 Shioaji",
        "cadence": "每盤落盤後",
        "owner": "微結構資料建置器",
        "window": 7 * 86400,
    },
    "tw-microstructure-captures-cold": {
        "title": "股票／期權即時 Tick 與五檔",
        "provider": "永豐 Shioaji",
        "cadence": "盤中連續",
        "owner": "Shioaji 即時擷取服務",
        "window": 7 * 86400,
    },
    "openbb-compact": {
        "title": "OpenBB 驗證封存",
        "provider": "OpenBB 多供應商",
        "cadence": "持續回補",
        "owner": "OpenBB 封存服務",
        "window": 15 * 60,
    },
    "openbb-task-shards-local": {
        "title": "OpenBB 可續傳任務分片",
        "provider": "OpenBB 多供應商",
        "cadence": "持續回補",
        "owner": "OpenBB 封存服務",
        "window": 15 * 60,
    },
    "yahoo-market": {
        "title": "Yahoo 市場資料",
        "provider": "yfinance / Yahoo Finance",
        "cadence": "每日與盤前補齊",
        "owner": "Yahoo OHLCV 更新器",
        "window": 72 * 3600,
    },
    "okx": {
        "title": "OKX 永續合約",
        "provider": "OKX",
        "cadence": "1 分鐘增量",
        "owner": "OKX 公開行情更新器",
        "window": 6 * 3600,
    },
    "bybit": {
        "title": "Bybit 永續合約",
        "provider": "Bybit",
        "cadence": "1 分鐘增量",
        "owner": "Bybit 公開行情更新器",
        "window": 6 * 3600,
    },
    "binance": {
        "title": "Binance USD-M 永續合約",
        "provider": "Binance",
        "cadence": "1 分鐘增量",
        "owner": "Binance 公開行情與特徵更新器",
        "window": 6 * 3600,
    },
    "binance-public-archive": {
        "title": "Binance 現貨與交割合約官方封存",
        "provider": "Binance Data Vision",
        "cadence": "每日官方日檔；月檔發布後重整",
        "owner": "Binance 官方封存校驗更新器",
        "window": 72 * 3600,
    },
    "crypto-reference": {
        "title": "加密資料唯一主來源",
        "provider": "設定 API / 免費原生來源",
        "cadence": "1 分鐘至每日，依來源新鮮度與月配額",
        "owner": "加密來源分配與去重更新器",
        "window": 26 * 3600,
    },
    "free-public-context": {
        "title": "免費公開市場脈絡",
        "provider": "DeFi / Bitcoin / Ethereum / Coin Metrics",
        "cadence": "1 分鐘、事件與每日",
        "owner": "免費公開來源快照更新器",
        "window": 26 * 3600,
    },
    "coinmetrics-community": {
        "title": "Coin Metrics Community 全量日資料",
        "provider": "Coin Metrics Community",
        "cadence": "每日增量與版本保存",
        "owner": "Coin Metrics Community 回補器",
        "window": 72 * 3600,
    },
    "dune-crypto": {
        "title": "Dune 鏈上歷史資料",
        "provider": "Dune",
        "cadence": "每日增量；歷史分區可續傳",
        "owner": "Dune 版本化 SQL 回補器",
        "window": 72 * 3600,
    },
    "crypto-etf-history": {
        "title": "SEC／ETF 發行商歷史資料",
        "provider": "SEC EDGAR / ETF 發行商",
        "cadence": "每日增量；申報事件與發行商日檔",
        "owner": "Crypto ETF 歷史回補器",
        "window": 72 * 3600,
    },
    "tw-index-futures": {
        "title": "TAIFEX 全期貨日資料",
        "provider": "TAIFEX",
        "cadence": "每個交易日收盤後",
        "owner": "TAIFEX 日資料計時器",
        "window": 72 * 3600,
    },
    "tw-index-derivatives-ticks": {
        "title": "TAIFEX 台指期權逐筆成交",
        "provider": "TAIFEX",
        "cadence": "官方近 30 交易日檔案",
        "owner": "TAIFEX 逐筆回補器",
        "window": 4 * 86400,
    },
    "tw-index-options-daily": {
        "title": "TAIFEX 選擇權日資料",
        "provider": "TAIFEX",
        "cadence": "每個交易日收盤後",
        "owner": "TAIFEX 選擇權回補器",
        "window": 72 * 3600,
    },
    "tw-futures": {
        "title": "永豐期貨歷史 Tick",
        "provider": "永豐 Shioaji / TAIFEX",
        "cadence": "配額允許時持續回補",
        "owner": "Shioaji 期貨歷史回補器",
        "window": 15 * 60,
    },
    "forex-frankfurter": {
        "title": "Frankfurter 外匯",
        "provider": "Frankfurter",
        "cadence": "每日",
        "owner": "Frankfurter 更新器",
        "window": 72 * 3600,
    },
    "forex-pepperstone": {
        "title": "Pepperstone 市場資料",
        "provider": "Pepperstone",
        "cadence": "每日",
        "owner": "Pepperstone 更新器",
        "window": 72 * 3600,
    },
    "legacy-parquet": {
        "title": "舊版 Parquet 封存",
        "provider": "歷史封存",
        "cadence": "凍結／待遷移",
        "owner": "人工稽核",
        "window": None,
    },
}

_SUMMARY_CANDIDATES: Final[dict[str, tuple[str, ...]]] = {
    "tw-public": ("download_summary.json",),
    "yahoo-market": (
        "download_summary.json",
        "daily_update_summary.json",
        "incremental_update_summary.json",
    ),
    "okx": ("1m/download_summary.json", "daily/download_summary.json", "download_summary.json"),
    "bybit": ("1m/download_summary.json", "daily/download_summary.json", "download_summary.json"),
    "binance": ("1m/download_summary.json", "daily/download_summary.json", "download_summary.json"),
    "binance-public-archive": ("download_summary.json", "plan_summary.json", "capacity_receipt.json"),
    "crypto-reference": ("download_summary.json", "source_status.json"),
    "free-public-context": ("download_summary.json",),
    "coinmetrics-community": ("download_summary.json",),
    "dune-crypto": ("download_summary.json",),
    "crypto-etf-history": ("download_summary.json",),
    "tw-index-futures": ("manifest.json",),
    "tw-index-derivatives-ticks": ("manifest.json",),
    "tw-index-options-daily": (
        "manifest.json",
        "manifest_weekly.json",
        "manifest_final_settlement.json",
    ),
    "tw-futures": ("shioaji_contracts/manifest.json",),
    "forex-frankfurter": ("download_summary.json",),
    "forex-pepperstone": ("download_summary.json",),
}

_STATUS_PRIORITY: Final[dict[str, int]] = {
    "blocked": 8,
    "unavailable": 7,
    "degraded": 6,
    "stale": 5,
    "waiting": 4,
    "updating": 3,
    "current": 2,
    "complete": 1,
    "deferred": 1,
    "legacy": 0,
}

_REFRESH_UNITS: Final[dict[str, dict[str, str | None]]] = {
    "registered_daily": {
        "service": "stockagent-registered-data-daily.service",
        "timer": "stockagent-registered-data-daily.timer",
    },
    "registered_intraday": {
        "service": "stockagent-registered-data-intraday.service",
        "timer": "stockagent-registered-data-intraday.timer",
    },
    "registered_backfill": {
        "service": "stockagent-registered-data-backfill.service",
        "timer": "stockagent-registered-data-backfill.timer",
    },
    "taifex_futures": {
        "service": "stockagent-taifex-futures-daily.service",
        "timer": "stockagent-taifex-futures-daily.timer",
    },
    "taifex_auxiliary": {
        "service": "stockagent-taifex-auxiliary-daily.service",
        "timer": "stockagent-taifex-auxiliary-daily.timer",
    },
    "shioaji_minute": {
        "service": "stockagent-shioaji-minute-backfill.service",
        "timer": "stockagent-shioaji-minute-backfill.timer",
    },
    "shioaji_fop_stream": {
        "service": "stockagent-shioaji-taifex-bidask.service",
        "timer": None,
    },
    "shioaji_stock_stream": {
        "service": "stockagent-shioaji-top200.service",
        "timer": None,
    },
    "shioaji_futures_history": {
        "service": "stockagent-shioaji-tx-history-backfill.service",
        "timer": None,
    },
    "openbb_archive": {
        "service": "stockagent-openbb-archive.service",
        "timer": None,
    },
    "openbb_l1_compaction": {
        "service": "stockagent-openbb-l1-compaction.service",
        "timer": "stockagent-openbb-l1-compaction.timer",
    },
    "binance_backfill": {
        "service": "stockagent-binance-public-backfill-v4.service",
        "timer": None,
    },
    "binance_public_archive": {
        "service": "stockagent-binance-public-archive.service",
        "timer": "stockagent-binance-public-archive.timer",
    },
    "tw_public_preopen": {
        "service": "stockagent-discord-bot.service",
        "timer": None,
    },
    "tw_public_publication": {
        "service": "stockagent-tw-public-publication-sweep.service",
        "timer": "stockagent-tw-public-publication-sweep.timer",
    },
    "tw_public_source_events": {
        "service": "stockagent-tw-public-source-events.service",
        "timer": None,
    },
    "tw_public_0830": {
        "service": "stockagent-tw-public-0830-check.service",
        "timer": "stockagent-tw-public-0830-check.timer",
    },
    "tw_public_eligibility": {
        "service": "stockagent-tw-day-trade-eligibility.service",
        "timer": "stockagent-tw-day-trade-eligibility.timer",
    },
    "tw_day_trade_preopen_gate": {
        "service": "stockagent-tw-day-trade-preopen-gate.service",
        "timer": "stockagent-tw-day-trade-preopen-gate.timer",
    },
}

_AUTOMATION_PROFILES: Final[dict[str, dict[str, Any]]] = {
    "group:tw-public": {
        "mode": "preopen_gate",
        "service_keys": (
            "tw_public_source_events",
            "tw_public_publication",
            "tw_public_0830",
            "tw_public_eligibility",
            "tw_day_trade_preopen_gate",
        ),
        "schedule_label": "156 項來源版本事件持續監測、07:50 全量掃描、08:00/08:20/08:29 嚴格發布與 08:59:30 最終守門",
        "active_means_running": False,
    },
    "group:tw-minute-train": {
        "mode": "timer",
        "service_keys": ("shioaji_minute",),
        "schedule_label": "交易日 14:45（Asia/Taipei）",
        "calendar_weekdays": True,
        "calendar_time": "14:45",
    },
    "group:tw-minute-source-cold": {
        "mode": "timer",
        "service_keys": ("shioaji_minute",),
        "schedule_label": "交易日 14:45（Asia/Taipei）",
        "calendar_weekdays": True,
        "calendar_time": "14:45",
    },
    "group:tw-microstructure-train": {
        "mode": "upstream_session",
        "service_keys": ("shioaji_stock_stream",),
        "schedule_label": "每個股票即時盤落盤後建置",
        "active_means_running": False,
    },
    "group:tw-microstructure-captures-cold": {
        "mode": "stream",
        "service_keys": ("shioaji_fop_stream", "shioaji_stock_stream"),
        "schedule_label": "依台股與 TAIFEX 交易時窗",
        "stream_kind": "mixed_tw",
        "active_means_running": False,
    },
    "group:openbb-compact": {
        "mode": "continuous_backfill",
        "service_keys": ("openbb_archive",),
        "schedule_label": "常駐回補；依供應商配額冷卻",
    },
    "group:openbb-task-shards-local": {
        "mode": "continuous_backfill",
        "service_keys": ("openbb_archive",),
        "schedule_label": "常駐回補；依供應商配額冷卻",
    },
    "group:yahoo-market": {
        "mode": "timer",
        "service_keys": ("registered_daily", "registered_intraday"),
        "schedule_label": "每日 06:30；Crypto 每輪完成後 1 分鐘",
        "calendar_weekdays": False,
        "calendar_time": "06:30",
    },
    "group:okx": {
        "mode": "interval_after_completion",
        "service_keys": ("registered_intraday", "registered_backfill"),
        "schedule_label": "尾端本輪完成後 1 分鐘；每週日 02:00 完整 head/backfill",
    },
    "group:bybit": {
        "mode": "interval_after_completion",
        "service_keys": ("registered_intraday", "registered_backfill"),
        "schedule_label": "尾端本輪完成後 1 分鐘；每週日 02:00 完整 head/backfill",
    },
    "group:binance": {
        "mode": "interval_after_completion",
        "service_keys": (
            "registered_intraday",
            "registered_backfill",
            "binance_backfill",
        ),
        "schedule_label": "尾端本輪完成後 1 分鐘；每週日 02:00 完整 head/backfill",
    },
    "group:binance-public-archive": {
        "mode": "daily_archive",
        "service_keys": ("binance_public_archive",),
        "schedule_label": "每日 12:30（Asia/Taipei）；官方月檔出現後自動重整",
        "calendar_weekdays": False,
        "calendar_time": "12:30",
    },
    "group:crypto-reference": {
        "mode": "interval_after_completion",
        "service_keys": ("registered_intraday",),
        "schedule_label": "每輪完成後；各端點再依 1 分鐘、15 分鐘或每日 cadence receipt 去重",
    },
    "group:free-public-context": {
        "mode": "interval_after_completion",
        "service_keys": ("registered_intraday",),
        "schedule_label": "本輪完成後 1 分鐘",
    },
    "group:coinmetrics-community": {
        "mode": "interval_after_completion",
        "service_keys": ("registered_intraday",),
        "schedule_label": "本輪完成後 1 分鐘",
    },
    "group:dune-crypto": {
        "mode": "timer",
        "service_keys": ("registered_daily",),
        "schedule_label": "每日 06:30（Asia/Taipei）；HTTP 402 時停止新增執行",
        "calendar_weekdays": False,
        "calendar_time": "06:30",
    },
    "group:crypto-etf-history": {
        "mode": "timer",
        "service_keys": ("registered_daily",),
        "schedule_label": "每日 06:30（Asia/Taipei）",
        "calendar_weekdays": False,
        "calendar_time": "06:30",
    },
    "group:tw-index-futures": {
        "mode": "timer",
        "service_keys": ("taifex_futures",),
        "schedule_label": "交易日 16:30（Asia/Taipei）",
        "calendar_weekdays": True,
        "calendar_time": "16:30",
    },
    "group:tw-index-derivatives-ticks": {
        "mode": "timer",
        "service_keys": ("taifex_auxiliary",),
        "schedule_label": "交易日 17:00（Asia/Taipei）",
        "calendar_weekdays": True,
        "calendar_time": "17:00",
    },
    "group:tw-index-options-daily": {
        "mode": "timer",
        "service_keys": ("taifex_auxiliary",),
        "schedule_label": "交易日 17:00（Asia/Taipei）",
        "calendar_weekdays": True,
        "calendar_time": "17:00",
    },
    "group:tw-futures": {
        "mode": "quota_backfill",
        "service_keys": ("shioaji_futures_history",),
        "schedule_label": "常駐回補；依實測流量重置證據續跑",
    },
    "group:forex-frankfurter": {
        "mode": "timer",
        "service_keys": ("registered_daily",),
        "schedule_label": "每日 06:30（Asia/Taipei）",
        "calendar_weekdays": False,
        "calendar_time": "06:30",
    },
    "group:forex-pepperstone": {
        "mode": "timer",
        "service_keys": ("registered_daily",),
        "schedule_label": "每日 06:30（Asia/Taipei）",
        "calendar_weekdays": False,
        "calendar_time": "06:30",
    },
    "group:legacy-parquet": {
        "mode": "frozen",
        "service_keys": (),
        "schedule_label": "凍結封存；不再自動更新",
        "active_means_running": False,
    },
}

_OPERATION_ORDER: Final[dict[str, int]] = {
    "catching_up": 0,
    "streaming": 1,
    "complete": 2,
    "unable": 3,
}
_OPERATION_LABELS: Final[dict[str, str]] = {
    "catching_up": "正在抓／還沒到最新",
    "streaming": "正在串流",
    "complete": "已完成／已到最新",
    "unable": "無法完成",
}
_ANSI_RE: Final[re.Pattern[str]] = re.compile(r"\x1b\[[0-9;?]*[ -/]*[@-~]")
_TQDM_RE: Final[re.Pattern[str]] = re.compile(
    r"(?P<label>(?:precheck|repair|download):[A-Za-z0-9_:-]+):\s*"
    r"(?P<percent>\d+)%\|[^\r\n]*?\|\s*"
    r"(?P<current>\d+)/(?P<total>\d+)\s*"
    r"\[(?P<elapsed>[0-9:]+)<(?P<remaining>[0-9:?]+),"
)
_REFRESH_SERVICE_SNAPSHOT_MAX_AGE_SECONDS: Final[int] = 180


def _read_json(path: Path, default: Any = None) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (FileNotFoundError, OSError, UnicodeError, json.JSONDecodeError):
        return default


def _systemd_time(value: Any) -> datetime | None:
    """Parse systemd's localized display timestamp without trusting ``CST``."""

    text = str(value or "").strip()
    if not text or text == "n/a":
        return None
    parts = text.split()
    if len(parts) < 3:
        return None
    try:
        parsed = datetime.fromisoformat(f"{parts[1]}T{parts[2]}")
    except ValueError:
        return None
    return parsed.replace(tzinfo=TAIPEI).astimezone(UTC)


def _systemd_properties(unit: str, properties: tuple[str, ...]) -> dict[str, str]:
    """Read allowlisted properties from one fixed unit."""

    fields: dict[str, str] = {}
    try:
        result = subprocess.run(
            (
                "systemctl",
                "show",
                unit,
                f"--property={','.join(properties)}",
                "--no-pager",
            ),
            check=False,
            capture_output=True,
            text=True,
            timeout=2.0,
        )
    except (OSError, subprocess.SubprocessError):
        result = None
    if result is not None and result.returncode == 0:
        for line in result.stdout.splitlines():
            key, separator, value = line.partition("=")
            if separator:
                fields[key] = value
    return fields


def _systemd_property_sets(
    units: tuple[str, ...], properties: tuple[str, ...]
) -> dict[str, dict[str, str]]:
    """Read all fixed units through one D-Bus round trip."""

    if not units:
        return {}
    try:
        result = subprocess.run(
            (
                "systemctl",
                "show",
                *units,
                f"--property=Id,{','.join(properties)}",
                "--no-pager",
            ),
            check=False,
            capture_output=True,
            text=True,
            timeout=3.0,
        )
    except (OSError, subprocess.SubprocessError):
        return {}
    output: dict[str, dict[str, str]] = {}
    fields: dict[str, str] = {}
    for line in [*result.stdout.splitlines(), ""]:
        if line:
            key, separator, value = line.partition("=")
            if separator:
                fields[key] = value
            continue
        unit_id = str(fields.get("Id") or "")
        if unit_id:
            output[unit_id] = fields
        fields = {}
    return output


def _fresh_refresh_service_snapshot(
    path: Path,
    *,
    now: datetime | None = None,
) -> dict[str, dict[str, Any]] | None:
    payload = _read_json(path, {})
    if not isinstance(payload, Mapping):
        return None
    try:
        generated = datetime.fromisoformat(
            str(payload.get("generated_at_utc") or "").replace("Z", "+00:00")
        )
    except ValueError:
        return None
    if generated.tzinfo is None:
        generated = generated.replace(tzinfo=UTC)
    observed = (now or datetime.now(UTC)).astimezone(UTC)
    age_seconds = (observed - generated.astimezone(UTC)).total_seconds()
    if age_seconds < -30 or age_seconds > _REFRESH_SERVICE_SNAPSHOT_MAX_AGE_SECONDS:
        return None
    services = payload.get("services")
    if not isinstance(services, Mapping):
        return None
    output: dict[str, dict[str, Any]] = {}
    for key, value in services.items():
        if not isinstance(value, Mapping):
            continue
        output[str(key)] = {
            **dict(value),
            "evidence_source": "systemd_snapshot",
            "evidence_generated_at_utc": generated.astimezone(UTC).isoformat(),
        }
    return output or None


def _service_state(
    unit: str,
    timer: str | None = None,
    *,
    property_sets: Mapping[str, Mapping[str, str]] | None = None,
) -> dict[str, Any]:
    """Read a fixed service and timer without exposing invocation identifiers."""

    fields = (
        dict(property_sets.get(unit, {}))
        if property_sets is not None
        else _systemd_properties(
            unit,
            (
                "ActiveState",
                "SubState",
                "NRestarts",
                "Result",
                "ExecMainStartTimestamp",
                "ExecMainExitTimestamp",
            ),
        )
    )
    active = fields.get("ActiveState") in {"active", "activating", "reloading"}
    if not fields:
        cgroup = Path("/sys/fs/cgroup/system.slice") / unit / "cgroup.procs"
        try:
            active = bool(cgroup.read_text(encoding="utf-8").strip())
        except OSError:
            active = False
        if active:
            fields = {"ActiveState": "active", "SubState": "running"}
    timer_fields = (
        dict(property_sets.get(timer, {}))
        if timer and property_sets is not None
        else _systemd_properties(
            timer,
            (
                "ActiveState",
                "SubState",
                "NextElapseUSecRealtime",
                "LastTriggerUSec",
            ),
        )
        if timer
        else {}
    )
    output = {
        "active": active,
        "state": fields.get("SubState") or fields.get("ActiveState") or "unknown",
        "restarts": _integer(fields.get("NRestarts")),
        "result": fields.get("Result") or None,
        "started_at_utc": _iso(_systemd_time(fields.get("ExecMainStartTimestamp"))),
        "completed_at_utc": _iso(_systemd_time(fields.get("ExecMainExitTimestamp"))),
        "timer_active": timer_fields.get("ActiveState") == "active",
        "timer_state": timer_fields.get("SubState")
        or timer_fields.get("ActiveState")
        or ("not_applicable" if not timer else "unknown"),
        "next_run_at_utc": _iso(
            _systemd_time(timer_fields.get("NextElapseUSecRealtime"))
        ),
        "last_trigger_at_utc": _iso(_systemd_time(timer_fields.get("LastTriggerUSec"))),
    }
    return output


def _refresh_service_states(
    *,
    snapshot_path: Path | None = None,
    now: datetime | None = None,
    prefer_snapshot: bool = False,
) -> dict[str, dict[str, Any]]:
    if prefer_snapshot and snapshot_path is not None:
        snapshot = _fresh_refresh_service_snapshot(snapshot_path, now=now)
        if snapshot is not None:
            return snapshot
    units = tuple(
        dict.fromkeys(
            str(unit)
            for values in _REFRESH_UNITS.values()
            for unit in (values.get("service"), values.get("timer"))
            if unit
        )
    )
    property_sets = _systemd_property_sets(
        units,
        (
            "ActiveState",
            "SubState",
            "NRestarts",
            "Result",
            "ExecMainStartTimestamp",
            "ExecMainExitTimestamp",
            "NextElapseUSecRealtime",
            "LastTriggerUSec",
        ),
    )
    if not property_sets and snapshot_path is not None:
        snapshot = _fresh_refresh_service_snapshot(snapshot_path, now=now)
        if snapshot is not None:
            return snapshot
    states = {
        name: _service_state(
            str(units["service"]),
            str(units["timer"]) if units.get("timer") else None,
            property_sets=property_sets,
        )
        for name, units in _REFRESH_UNITS.items()
    }
    evidence_source = "systemd_live" if property_sets else "cgroup_fallback"
    for state in states.values():
        state["evidence_source"] = evidence_source
    return states


def _duration_seconds(value: str) -> int | None:
    if not value or "?" in value:
        return None
    try:
        parts = [int(part) for part in value.split(":")]
    except ValueError:
        return None
    if len(parts) == 2:
        return parts[0] * 60 + parts[1]
    if len(parts) == 3:
        return parts[0] * 3600 + parts[1] * 60 + parts[2]
    return None


def _tail_text(path: Path, maximum_bytes: int = 2 * 1024 * 1024) -> str:
    try:
        with path.open("rb") as handle:
            size = handle.seek(0, 2)
            handle.seek(max(0, size - maximum_bytes))
            return handle.read().decode("utf-8", errors="replace")
    except OSError:
        return ""


def _runtime_progress(repo_root: Path) -> list[dict[str, Any]]:
    roots = (
        repo_root / "artifacts/daily_downloader/registered_daily",
        repo_root / "artifacts/daily_downloader/registered_intraday",
    )
    paths: list[Path] = []
    for directory in roots:
        try:
            candidates = sorted(
                directory.glob("*.log"),
                key=lambda path: path.stat().st_mtime,
                reverse=True,
            )
        except OSError:
            candidates = []
        paths.extend(candidates[:1])
    output: list[dict[str, Any]] = []
    for path in paths:
        text = _ANSI_RE.sub("", _tail_text(path).replace("\r", "\n"))
        latest_by_label: dict[str, dict[str, Any]] = {}
        for match in _TQDM_RE.finditer(text):
            current = int(match.group("current"))
            total = int(match.group("total"))
            if total <= 0:
                continue
            remaining = _duration_seconds(match.group("remaining"))
            if remaining == 0 and current < total:
                remaining = 1
            latest_by_label[match.group("label")] = {
                "label": match.group("label"),
                "current": min(current, total),
                "total": total,
                "ratio": min(1.0, max(0.0, current / total)),
                "remaining_seconds": remaining,
            }
        output.extend(latest_by_label.values())
    return output


def _select_runtime_progress(
    rows: Iterable[Mapping[str, Any]],
    *,
    tokens: tuple[str, ...],
) -> dict[str, Any] | None:
    candidates = [
        dict(row)
        for row in rows
        if any(token in str(row.get("label") or "") for token in tokens)
        and (_integer(row.get("current")) or 0) < (_integer(row.get("total")) or 0)
    ]
    if not candidates:
        return None
    # Aggregated groups finish only when their slowest active phase finishes.
    return max(
        candidates,
        key=lambda row: (
            _integer(row.get("remaining_seconds")) or -1,
            _integer(row.get("total")) or 0,
        ),
    )


def _integer(value: Any) -> int | None:
    if isinstance(value, bool):
        return None
    try:
        number = int(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return number if number >= 0 else None


def _number(value: Any) -> float | None:
    if isinstance(value, bool):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return number if math.isfinite(number) else None


def _parse_time(value: Any) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        # A date-only ``data_through`` value means coverage through that UTC
        # date, not the first instant of it. Python accepts YYYY-MM-DD as
        # midnight, which made a successful 06:30 refresh appear stale roughly
        # one day too early.
        parsed = datetime.fromisoformat(
            f"{text}T23:59:59+00:00"
            if re.fullmatch(r"\d{4}-\d{2}-\d{2}", text)
            else text.replace("Z", "+00:00")
        )
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _mtime(path: Path) -> datetime | None:
    try:
        return datetime.fromtimestamp(path.stat().st_mtime, tz=UTC)
    except OSError:
        return None


def _iso(value: datetime | None) -> str | None:
    return value.isoformat().replace("+00:00", "Z") if value else None


def _latest_time(
    paths: Iterable[Path], payloads: Iterable[Any] = ()
) -> datetime | None:
    values = [value for value in (_mtime(path) for path in paths) if value is not None]
    for payload in payloads:
        if not isinstance(payload, Mapping):
            continue
        for key in (
            "generated_at_utc",
            "generated_at",
            "completed_at_utc",
            "completed_at_taipei",
            "updated_at",
        ):
            parsed = _parse_time(payload.get(key))
            if parsed is not None:
                values.append(parsed)
    return max(values) if values else None


def _coverage(
    current: Any,
    total: Any,
    *,
    unit: str,
    label: str,
) -> dict[str, Any] | None:
    current_value = _integer(current)
    total_value = _integer(total)
    if current_value is None or total_value is None or total_value <= 0:
        return None
    return {
        "current": min(current_value, total_value),
        "total": total_value,
        "ratio": min(1.0, max(0.0, current_value / total_value)),
        "unit": unit,
        "label": label,
    }


def _complete_eta(basis: str = "來源稽核已完成。") -> dict[str, Any]:
    return {
        "state": "complete",
        "remaining_seconds": 0,
        "estimated_complete_at_utc": None,
        "confidence": "high",
        "basis": basis,
    }


def _unknown_eta(state: str, basis: str) -> dict[str, Any]:
    return {
        "state": state,
        "remaining_seconds": None,
        "estimated_complete_at_utc": None,
        "confidence": "not_available",
        "basis": basis,
    }


def _freshness(
    latest: datetime | None,
    *,
    now: datetime,
    window_seconds: int | None,
    continuous: bool = False,
) -> dict[str, Any]:
    age = max(0.0, (now - latest).total_seconds()) if latest else None
    if continuous:
        state = "continuous"
    elif latest is None or window_seconds is None:
        state = "not_applicable" if window_seconds is None else "unknown"
    else:
        state = "current" if age <= window_seconds else "stale"
    return {
        "state": state,
        "age_seconds": round(age, 3) if age is not None else None,
        "threshold_seconds": window_seconds,
    }


def _status_counts(payloads: Iterable[Any]) -> dict[str, int]:
    output: dict[str, int] = {}

    def walk(value: Any) -> None:
        if isinstance(value, Mapping):
            for key, item in value.items():
                if str(key) == "status_counts" and isinstance(item, Mapping):
                    for raw_status, raw_count in item.items():
                        count = _integer(raw_count)
                        if count is not None:
                            status = str(raw_status)
                            output[status] = output.get(status, 0) + count
                elif isinstance(item, Mapping):
                    walk(item)

    for payload in payloads:
        walk(payload)
    return output


def _extract_data_through(payloads: Iterable[Any]) -> str | None:
    candidates: list[str] = []
    for payload in payloads:
        if not isinstance(payload, Mapping):
            continue
        for key in (
            "applied_end_date",
            "provider_end_date",
            "end_date",
            "date_end",
        ):
            value = str(payload.get(key) or "").strip()
            if value:
                candidates.append(value)
        quality = payload.get("quality")
        if isinstance(quality, Mapping):
            value = str(quality.get("last_date") or "").strip()
            if value:
                candidates.append(value)
    return max(candidates) if candidates else None


def _extract_rows(payloads: Iterable[Any]) -> int | None:
    candidates: list[int] = []
    for payload in payloads:
        if not isinstance(payload, Mapping):
            continue
        for key in (
            "stored_row_count",
            "rows_total",
            "row_count",
            "success_rows",
            "rows",
            "rows_this_run",
        ):
            value = _integer(payload.get(key))
            if value is not None:
                candidates.append(value)
        quality = payload.get("quality")
        if isinstance(quality, Mapping):
            value = _integer(quality.get("rows"))
            if value is not None:
                candidates.append(value)
        all_futures = payload.get("all_futures_daily")
        if isinstance(all_futures, Mapping) and isinstance(
            all_futures.get("quality"), Mapping
        ):
            value = _integer(all_futures["quality"].get("rows"))
            if value is not None:
                candidates.append(value)
    return max(candidates) if candidates else None


def _generic_group(
    root: Path,
    config: Mapping[str, Any],
    *,
    now: datetime,
) -> dict[str, Any]:
    dataset = str(config.get("dataset") or "unknown")
    meta = _GROUP_META.get(dataset, {})
    source = str(config.get("source") or "")
    source_root = root / source
    candidates = [
        source_root / relative for relative in _SUMMARY_CANDIDATES.get(dataset, ())
    ]
    payloads = [_read_json(path) for path in candidates]
    payloads = [payload for payload in payloads if payload is not None]
    progress_path = (
        source_root / "1m/progress.json"
        if dataset in {"okx", "bybit", "binance"}
        else source_root / "progress.json"
    )
    progress_payload = _read_json(progress_path, {})
    latest = _latest_time(candidates, payloads)
    data_through = _extract_data_through(payloads)
    data_through_time = _parse_time(data_through)
    # Daily datasets are fresh only through their audited data date.  A newly
    # rewritten manifest must not make old market rows look current.
    intraday_current_day = (
        dataset in {"okx", "bybit", "binance"}
        and data_through_time is not None
        and data_through_time.date() >= now.date()
    )
    if (
        data_through_time is not None
        and dataset
        not in {
            "openbb-compact",
            "openbb-task-shards-local",
        }
        and not intraday_current_day
    ):
        latest = data_through_time
    window = meta.get("window")
    fresh = _freshness(
        latest,
        now=now,
        window_seconds=int(window) if isinstance(window, int) else None,
    )
    counts = _status_counts(payloads)
    total = sum(counts.values())
    failure = sum(
        count
        for status, count in counts.items()
        if any(
            token in status.lower()
            for token in (
                "fail",
                "error",
                "partial",
                "mismatch",
                "repair",
                "quarantine",
                "invalid",
                "blocked",
            )
        )
    )
    batch_incomplete = any(
        str(payload.get("state") or "").lower()
        in {"failed", "error", "blocked", "partial"}
        for payload in payloads
        if isinstance(payload, Mapping)
    )
    completed = max(0, total - failure)
    coverage = _coverage(completed, total, unit="項", label="最近批次")

    if not source_root.exists():
        status = "unavailable"
        status_label = "資料根目錄不存在"
    elif dataset == "legacy-parquet":
        status = "legacy"
        status_label = "凍結封存"
    elif failure or batch_incomplete:
        status = "degraded"
        status_label = (
            f"最近批次有 {failure:,} 個失敗"
            if failure
            else "最近批次未完整收斂"
        )
    elif fresh["state"] == "stale":
        status = "stale"
        status_label = "需要補到最新"
    elif latest is None:
        status = "degraded"
        status_label = "缺少可驗證摘要"
    else:
        status = "current"
        status_label = "在新鮮度範圍內"

    if (
        status in {"current", "legacy"}
        and (coverage is None or failure == 0)
        and not batch_incomplete
    ):
        eta = _complete_eta("最近可驗證批次沒有待處理失敗。")
    elif status == "unavailable":
        eta = _unknown_eta("blocked", "資料根目錄不存在，無法開始估算。")
    else:
        eta = _unknown_eta(
            "waiting_schedule",
            "尚無執行中吞吐率；更新器開始並寫入進度後才能估算。",
        )

    if isinstance(progress_payload, Mapping):
        progress_state = str(progress_payload.get("state") or "").lower()
        progress_updated = _parse_time(progress_payload.get("updated_at_utc"))
        progress_age = (
            max(0.0, (now - progress_updated).total_seconds())
            if progress_updated is not None
            else None
        )
        progress_live = (
            progress_state == "running"
            and progress_age is not None
            and progress_age <= 15 * 60
        )
        if progress_live:
            progress_phase = str(progress_payload.get("phase") or "")
            raw_progress_counts = progress_payload.get("status_counts")
            raw_current = _integer(progress_payload.get("current"))
            raw_total = _integer(progress_payload.get("total"))
            legacy_page_denominator_saturated = bool(
                isinstance(raw_progress_counts, Mapping)
                and (_integer(raw_progress_counts.get("page_fetched")) or 0) > 0
                and raw_current is not None
                and raw_total is not None
                and raw_total > 0
                and raw_current >= raw_total
                and progress_phase != "complete"
            )
            status = "updating"
            status_label = (
                "正在建立精確容量計畫"
                if progress_phase == "discover"
                else str(progress_payload.get("label") or "資料更新正在執行")
            )
            coverage = (
                None
                if legacy_page_denominator_saturated
                else _coverage(
                    progress_payload.get("current"),
                    progress_payload.get("total"),
                    unit=str(progress_payload.get("unit") or "項"),
                    label="目前批次",
                )
            )
            remaining = (
                None
                if legacy_page_denominator_saturated
                else _integer(progress_payload.get("remaining_seconds"))
            )
            eta = {
                "state": "estimating" if remaining is not None else "warming_up",
                "remaining_seconds": remaining,
                "estimated_complete_at_utc": progress_payload.get(
                    "estimated_complete_at_utc"
                ),
                "confidence": "low" if remaining is not None else "not_available",
                "basis": (
                    "舊版進度把可變長度 request page 混入固定分母，分母已飽和但工作仍在執行；"
                    "本輪 ETA 保持未知，下一輪改用 symbol／feature-stage 邏輯單位。"
                    if legacy_page_denominator_saturated
                    else
                    "僅為官方目錄規劃階段 ETA；全量下載會在物件傳輸開始後"
                    "依實測吞吐重新估算。"
                    if progress_phase == "discover"
                    else str(
                        progress_payload.get("basis")
                        or "依目前完整批次吞吐率線性外推。"
                    )
                ),
                "phase": progress_phase or None,
            }
            latest = progress_updated
        elif progress_state in {"failed", "partial"} and (
            progress_age is None or progress_age <= 7 * 86400
        ):
            status = "degraded"
            status_label = (
                "最近更新批次未完整收斂"
                if progress_state == "partial"
                else "最近更新批次失敗"
            )
            eta = _unknown_eta("waiting_schedule", "等待下一輪重試與新回執。")

    warning = []
    if failure or batch_incomplete:
        warning.append("最近一次摘要含失敗項目；成功檔案不代表整批完整。")
    if isinstance(progress_payload, Mapping):
        live_counts = progress_payload.get("status_counts")
        live_failures = 0
        if isinstance(live_counts, Mapping):
            live_failures = sum(
                _integer(count) or 0
                for item_status, count in live_counts.items()
                if any(
                    token in str(item_status).lower()
                    for token in (
                        "fail",
                        "error",
                        "partial",
                        "mismatch",
                        "repair",
                        "quarantine",
                        "invalid",
                        "blocked",
                    )
                )
            )
        if live_failures:
            warning.append(
                f"目前批次已有 {live_failures:,} 個失敗／部分完成項；"
                "更新器仍會完成其餘工作並保留錯誤明細。"
            )
        if (
            str(progress_payload.get("state") or "").lower() == "running"
            and isinstance(live_counts, Mapping)
            and (_integer(live_counts.get("page_fetched")) or 0) > 0
            and (_integer(progress_payload.get("total")) or 0) > 0
            and (_integer(progress_payload.get("current")) or 0)
            >= (_integer(progress_payload.get("total")) or 0)
            and str(progress_payload.get("phase") or "") != "complete"
        ):
            warning.append(
                "舊版 request-page 計數已塞滿分母但工作尚未結束；不顯示假 100% 或假 ETA。"
            )
    if not payloads and source_root.exists():
        warning.append("已登錄資料根，但尚未找到可驗證的摘要或 manifest。")
    return {
        "id": f"group:{dataset}",
        "parent_id": None,
        "scope": "storage_group",
        "title": str(meta.get("title") or dataset),
        "provider": str(meta.get("provider") or "其他"),
        "category": str(config.get("role") or "unknown"),
        "status": status,
        "status_label": status_label,
        "cadence": str(meta.get("cadence") or "依來源排程"),
        "update_owner": str(meta.get("owner") or "未指定"),
        "latest_at_utc": _iso(latest),
        "data_through": data_through,
        "freshness": fresh,
        "coverage": coverage,
        "eta": eta,
        "rows": _extract_rows(payloads),
        "publishable": bool(config.get("publish")),
        "automation_eligible": dataset != "legacy-parquet",
        "detail": str(config.get("note") or "已登錄資料群組。"),
        "warnings": warning,
        "detail_link": None,
    }


def _tw_public_publication_index(root: Path) -> dict[str, list[dict[str, Any]]]:
    """Index source-specific publication sweeps without inventing release SLAs.

    A phase receipt proves when our detector ran and which official product
    boundary motivated it.  It does not prove that every selected endpoint was
    published at that clock time, so callers must keep ``probe_boundary`` and
    actual content-change timestamps separate.
    """

    receipt_root = root / "artifacts/data_refresh/tw_public/publications"
    output: dict[str, list[dict[str, Any]]] = {}
    try:
        receipt_paths = sorted(receipt_root.glob("*/latest.json"))
    except OSError:
        receipt_paths = []
    for receipt_path in receipt_paths:
        payload = _read_json(receipt_path, {})
        if not isinstance(payload, Mapping):
            continue
        phase = str(payload.get("phase") or receipt_path.parent.name)
        boundary = str(payload.get("scheduled_boundary") or "").strip()
        selected = payload.get("selected_datasets")
        if not isinstance(selected, list):
            continue
        changed = {
            str(item.get("dataset") or "")
            for item in payload.get("changed_datasets", [])
            if isinstance(item, Mapping)
        }
        receipt = {
            "phase": phase,
            "scheduled_boundary": boundary or None,
            "official_basis": str(payload.get("official_basis") or "") or None,
            "last_started_at_utc": _iso(
                _parse_time(payload.get("started_at_taipei"))
            ),
            "last_completed_at_utc": _iso(
                _parse_time(payload.get("completed_at_taipei"))
            ),
            "last_status": str(payload.get("status") or "unknown"),
        }
        for raw_name in selected:
            name = str(raw_name or "").strip()
            if not name:
                continue
            output.setdefault(name, []).append(
                {**receipt, "content_change_observed": name in changed}
            )
    for rows in output.values():
        rows.sort(
            key=lambda item: (
                str(item.get("scheduled_boundary") or "99:99:99"),
                str(item.get("phase") or ""),
            )
        )
    return output


def _tw_public_sources(root: Path, *, now: datetime) -> list[dict[str, Any]]:
    base = root / "data_tw_public"
    manifest = _read_json(base / "dataset_manifest.json", [])
    summary = _read_json(base / "download_summary.json", {})
    receipt = _read_json(root / "artifacts/data_refresh/tw_public/latest.json", {})
    event_receipt = _read_json(
        root / "artifacts/data_refresh/tw_public/events/latest.json", {}
    )
    event_rows = (
        event_receipt.get("datasets", {})
        if isinstance(event_receipt, Mapping)
        else {}
    )
    event_rows = event_rows if isinstance(event_rows, Mapping) else {}
    publication_rows = _tw_public_publication_index(root)
    if not isinstance(manifest, list):
        return []
    report: dict[str, dict[str, str]] = {}
    try:
        with (base / "download_report.csv").open(
            encoding="utf-8", newline=""
        ) as handle:
            for row in csv.DictReader(handle):
                report[str(row.get("dataset") or "")] = dict(row)
    except (FileNotFoundError, OSError, UnicodeError, csv.Error):
        pass
    generated = _latest_time(
        [
            base / "download_summary.json",
            root / "artifacts/data_refresh/tw_public/latest.json",
        ],
        [summary, receipt],
    )
    fresh = _freshness(generated, now=now, window_seconds=72 * 3600)
    source_unavailable = (
        summary.get("source_unavailable_by_dataset", {})
        if isinstance(summary, Mapping)
        else {}
    )
    rows: list[dict[str, Any]] = []
    for item in manifest:
        if not isinstance(item, Mapping):
            continue
        name = str(item.get("name") or "").strip()
        if not name:
            continue
        audit = report.get(name, {})
        raw_status = str(audit.get("status") or "unknown").lower()
        failed = _integer(audit.get("failed_dates")) or 0
        missing = _integer(audit.get("missing_dates_after")) or 0
        coverage_flag = str(audit.get("coverage_complete", "")).lower()
        complete = coverage_flag == "true" or (
            raw_status in {"ok", "up_to_date", "complete"}
            and failed == 0
            and missing == 0
        )
        event_row = event_rows.get(name, {})
        event_row = event_row if isinstance(event_row, Mapping) else {}
        publication_receipts = publication_rows.get(name, [])
        event_applied = bool(
            event_row.get("last_probe_status") == "ok"
            and event_row.get("observed_version")
            and event_row.get("observed_version") == event_row.get("applied_version")
        )
        if event_row and not event_applied:
            status = "degraded"
            label = (
                "來源探測失敗"
                if event_row.get("last_probe_status") == "failed"
                else "已發現新版本，等待下載驗證"
            )
            eta = _unknown_eta(
                "running_unmeasured",
                "來源事件監測器會持續重試，直到下載與驗證收據成功。",
            )
        elif failed or missing or raw_status in {"failed", "incomplete", "error"}:
            status = "degraded"
            label = f"缺 {missing:,} 日／失敗 {failed:,} 日"
            eta = _unknown_eta(
                "waiting_schedule",
                "等待不可變公開資料更新器再次執行完整缺口掃描。",
            )
        elif complete and fresh["state"] == "current":
            status = "current"
            label = "完整且在新鮮度範圍內"
            eta = _complete_eta("缺口稽核為零，且最新快照已發佈。")
        elif complete:
            status = "stale"
            label = "歷史完整但需要更新"
            eta = _unknown_eta(
                "waiting_schedule",
                "目前沒有執行中吞吐率；下次更新會先掃描再補齊。",
            )
        else:
            status = "degraded"
            label = "缺少完整度證據"
            eta = _unknown_eta("unknown", "缺少逐資料集完整度回執。")
        warnings = []
        if not event_row:
            warnings.append("尚未取得此資料集的來源版本事件監測證據。")
        if isinstance(source_unavailable, Mapping) and name in source_unavailable:
            warnings.append("含官方來源已確認不可取得的日期；未將其偽裝成成功資料。")
        tags = item.get("tags") if isinstance(item.get("tags"), list) else []
        provider = str(item.get("source") or "臺灣官方來源")
        if "mops" in name.lower() or any(str(tag).lower() == "mops" for tag in tags):
            provider = "MOPS / 公開資訊觀測站"
        rows.append(
            {
                "id": f"tw-public:{name}",
                "parent_id": "group:tw-public",
                "scope": "logical_source",
                "title": name,
                "provider": provider,
                "category": ", ".join(str(tag) for tag in tags[:3]),
                "status": status,
                "status_label": label,
                "cadence": (
                    f"來源版本每 {int(event_row.get('interval_seconds'))} 秒探測"
                    if _integer(event_row.get("interval_seconds"))
                    else "來源版本持續探測"
                ),
                "update_owner": "來源事件監測器＋不可變快照更新器",
                "latest_at_utc": _iso(
                    _parse_time(event_row.get("last_checked_at_taipei"))
                    or generated
                ),
                "data_through": str(summary.get("end_date") or "") or None,
                "freshness": dict(fresh),
                "coverage": _coverage(
                    1 if complete else 0, 1, unit="資料集", label="缺口稽核"
                ),
                "eta": eta,
                "rows": _integer(audit.get("rows")),
                "publishable": True,
                "automation_eligible": True,
                "detail": (
                    str(item.get("description") or "官方公開資料集。")
                    + " 來源變更只有在定向下載、解析與驗證成功後才會確認套用。"
                ),
                "warnings": warnings,
                "detail_link": None,
                "_publication_hint": {
                    "schedule_kind": "probe_boundary",
                    "schedule_label": (
                        "來源版本持續探測；另於 "
                        + " / ".join(
                            str(item.get("scheduled_boundary") or "")[:5]
                            for item in publication_receipts
                            if item.get("scheduled_boundary")
                        )
                        + "（Asia/Taipei）執行公布邊界掃描"
                        if publication_receipts
                        else "來源版本持續探測；來源未承諾固定發布時刻"
                    ),
                    "exact_time_declared": False,
                    "probe_boundaries_taipei": [
                        str(item.get("scheduled_boundary") or "")
                        for item in publication_receipts
                        if item.get("scheduled_boundary")
                    ],
                    "detected_at_utc": _iso(
                        _parse_time(event_row.get("last_changed_at_taipei"))
                    ),
                    "last_checked_at_utc": _iso(
                        _parse_time(event_row.get("last_checked_at_taipei"))
                    ),
                    "applied_at_utc": _iso(
                        _parse_time(event_row.get("last_applied_at_taipei"))
                    ),
                    "next_check_at_utc": _iso(
                        _parse_time(event_row.get("next_probe_at_taipei"))
                    ),
                    "basis": (
                        "固定時刻是具名產品的掃描邊界；同批相關端點以實際內容版本變更時間為準，"
                        "不把掃描時間冒充成每個來源的官方發布 SLA。"
                    ),
                    "receipt_phases": publication_receipts,
                },
            }
        )
    return rows


def _shioaji_sources(
    status: Mapping[str, Any], *, now: datetime
) -> list[dict[str, Any]]:
    output: list[dict[str, Any]] = []
    pipelines = status.get("pipelines")
    if not isinstance(pipelines, list):
        return output
    status_map = {
        "active": "updating",
        "ready": "current",
        "waiting": "waiting",
        "attention": "degraded",
        "blocked": "blocked",
    }
    for pipeline in pipelines:
        if not isinstance(pipeline, Mapping):
            continue
        raw_status = str(pipeline.get("status") or "unavailable").lower()
        row_status = status_map.get(raw_status, "unavailable")
        latest = _parse_time(pipeline.get("latest_at_utc"))
        eta = pipeline.get("eta")
        coverage = pipeline.get("coverage")
        output.append(
            {
                "id": f"shioaji:{pipeline.get('id')}",
                "parent_id": "group:tw-futures"
                if pipeline.get("id") == "futures_history"
                else "group:tw-microstructure-captures-cold",
                "scope": "logical_source",
                "title": str(pipeline.get("title") or pipeline.get("id") or "Shioaji"),
                "provider": "永豐 Shioaji",
                "category": str(pipeline.get("category") or "market-data"),
                "status": row_status,
                "status_label": str(pipeline.get("status_label") or raw_status),
                "cadence": "盤中連續"
                if pipeline.get("category") == "realtime"
                else "配額允許時持續回補",
                "update_owner": "Shioaji 資料服務",
                "latest_at_utc": _iso(latest),
                "data_through": None,
                "freshness": _freshness(
                    latest,
                    now=now,
                    window_seconds=None
                    if pipeline.get("category") == "realtime"
                    else 72 * 3600,
                    continuous=pipeline.get("category") == "realtime",
                ),
                "coverage": dict(coverage) if isinstance(coverage, Mapping) else None,
                "eta": dict(eta)
                if isinstance(eta, Mapping)
                else _unknown_eta("unknown", "來源尚未提供 ETA 證據。"),
                "rows": None,
                "publishable": False,
                "automation_eligible": pipeline.get("category") != "on_demand",
                "detail": str(pipeline.get("detail") or "Shioaji 資料管線。"),
                "warnings": [str(value) for value in pipeline.get("warnings", [])],
                "detail_link": "../shioaji/",
            }
        )
    return output


def _openbb_sources(
    status: Mapping[str, Any], *, now: datetime
) -> list[dict[str, Any]]:
    providers = status.get("providers")
    if not isinstance(providers, list):
        providers = []
    latest = now
    source_age = _number(status.get("source_age_seconds"))
    if source_age is not None:
        latest = now - timedelta(seconds=source_age)
    output: list[dict[str, Any]] = []
    for provider in providers:
        if not isinstance(provider, Mapping):
            continue
        accepted = _integer(provider.get("accepted_tasks")) or 0
        backlog = _integer(provider.get("exclusive_backlog_tasks"))
        if backlog is None:
            backlog = _integer(provider.get("eligible_backlog_tasks")) or 0
        rate = _number(provider.get("recent_tasks_per_minute")) or 0.0
        active = (_integer(provider.get("active")) or 0) > 0
        cooldown = provider.get("cooldown") is True
        if backlog == 0:
            row_status = "complete"
            label = "目前無待處理任務"
            eta = _complete_eta("此供應商目前沒有專屬待處理任務。")
        elif active or rate > 0:
            row_status = "updating"
            label = "正在回補"
            seconds = int(math.ceil(backlog / rate * 60)) if rate > 0 else None
            eta = {
                "state": "estimating" if seconds is not None else "unknown",
                "remaining_seconds": seconds,
                "estimated_complete_at_utc": _iso(now + timedelta(seconds=seconds))
                if seconds is not None
                else None,
                "confidence": "low",
                "basis": "依此供應商最近接受任務速率與專屬 backlog 線性外推；配額會改變結果。",
            }
        elif cooldown:
            row_status = "waiting"
            label = "等待配額解除"
            eta = _unknown_eta(
                "waiting_quota",
                "配額冷卻中；在恢復有效吞吐率前不提供假完成時間。",
            )
        else:
            row_status = "waiting"
            label = "等待排程"
            eta = _unknown_eta("waiting_schedule", "目前速率為零，無法可靠外推。")
        output.append(
            {
                "id": f"openbb:{provider.get('provider')}",
                "parent_id": "group:openbb-compact",
                "scope": "logical_source",
                "title": str(provider.get("provider") or "OpenBB provider"),
                "provider": "OpenBB",
                "category": "archive-provider",
                "status": row_status,
                "status_label": label,
                "cadence": "持續回補",
                "update_owner": "OpenBB 供應商排程器",
                "latest_at_utc": _iso(latest),
                "data_through": None,
                "freshness": _freshness(latest, now=now, window_seconds=15 * 60),
                "coverage": _coverage(
                    accepted,
                    accepted + backlog,
                    unit="任務",
                    label="已接受／專屬待處理",
                ),
                "eta": eta,
                "rows": _integer(provider.get("success_rows")),
                "publishable": True,
                "automation_eligible": True,
                "detail": f"最近 {rate:,.1f} 任務/分鐘；專屬待處理 {backlog:,}。",
                "warnings": ["供應商 unavailable 與成功資料分開計數。"]
                if cooldown
                else [],
                "detail_link": "../openbb/",
            }
        )
    l1 = status.get("l1_compaction")
    if isinstance(l1, Mapping) and l1.get("generated_at_utc") is not None:
        success_files = _integer(l1.get("success_files")) or 0
        compacted_files = _integer(l1.get("compacted_files")) or 0
        pending_files = max(
            _integer(l1.get("pending_files")) or 0,
            success_files - compacted_files,
        )
        source_age = _number(l1.get("source_age_seconds"))
        latest = now - timedelta(seconds=source_age) if source_age is not None else None
        if pending_files == 0:
            row_status = "complete"
            status_label = "目前成功 shard 已全部壓實"
            eta = _complete_eta("目前沒有待壓實的成功 shard。")
        elif source_age is None or source_age > 2 * 3600:
            row_status = "stale"
            status_label = "壓實狀態逾時"
            eta = _unknown_eta(
                "stale_status", "L1 狀態超過兩小時未更新，不能可靠估計完成時間。"
            )
        else:
            row_status = "waiting"
            status_label = "等待下一輪增量壓實"
            # The installed timer starts at most 20,000 new shards per run and
            # is scheduled every 30 minutes. This is a low-confidence
            # no-new-arrivals capacity projection, not a completion promise.
            runs = math.ceil(pending_files / 20_000)
            seconds = runs * 30 * 60
            eta = {
                "state": "estimating",
                "remaining_seconds": seconds,
                "estimated_complete_at_utc": _iso(now + timedelta(seconds=seconds)),
                "confidence": "low",
                "basis": "依每半小時最多 20,000 shard 且沒有新資料的容量估計；小於 128 檔的 endpoint tail 會等待累積。",
            }
        source_bytes = _integer(l1.get("source_bytes")) or 0
        output_bytes = _integer(l1.get("output_bytes")) or 0
        reduction = (
            100.0 * (1.0 - output_bytes / source_bytes) if source_bytes else 0.0
        )
        output.append(
            {
                "id": "openbb:l1-compaction",
                "parent_id": "group:openbb-compact",
                "scope": "logical_source",
                "title": "OpenBB L1 Parquet 壓實",
                "provider": "本機 DuckDB／Polars／PyArrow",
                "category": "storage-compaction",
                "status": row_status,
                "status_label": status_label,
                "cadence": "每半小時",
                "update_owner": "OpenBB L1 壓實 timer",
                "latest_at_utc": _iso(latest),
                "data_through": None,
                "freshness": _freshness(latest, now=now, window_seconds=2 * 3600),
                "coverage": _coverage(
                    compacted_files,
                    success_files,
                    unit="shard",
                    label="已壓實／成功 L0",
                ),
                "eta": eta,
                "rows": _integer(l1.get("compacted_rows")),
                "publishable": True,
                "automation_eligible": True,
                "detail": (
                    f"active segments {_integer(l1.get('active_segments')) or 0:,}；"
                    f"待壓實 {pending_files:,} shards；已壓實來源空間縮減 {reduction:.2f}%。"
                ),
                "warnings": [
                    "L1 是 shadow query layer；L0 原始 shard 保留且未刪除。"
                ],
                "detail_link": "../openbb/",
            }
        )
    return output


def _crypto_feature_sources(root: Path, *, now: datetime) -> list[dict[str, Any]]:
    """Project exchange catalogs without pretending deferred archives are complete."""

    native_five_minute_prefixes = {
        "open_interest",
        "contract_taker_volume",
        "taker_buy_sell_volume",
        "contract_long_short_account_ratio",
        "global_long_short_account_ratio",
        "top_trader_account_ratio",
        "top_trader_position_ratio",
        "basis",
    }

    def migrate_legacy_catalog(source: Mapping[str, Any]) -> dict[str, Any]:
        # Legacy catalog files remain provenance for the still-running 15m job.
        # Reuse only their endpoint inventory, then relabel definitions according
        # to the new canonical 1m / native-5m contract.  Their mtimes are not used
        # as evidence that the new 1m dataset has run.
        text = json.dumps(source, ensure_ascii=False)
        migrated = json.loads(
            text.replace("15-minute", "one-minute").replace("15m", "1m")
        )
        migrated["bar"] = "1m"
        migrated["generated_at"] = None
        migrated["generated_at_utc"] = None
        for item in migrated.get("catalog", []):
            if not isinstance(item, dict):
                continue
            source_id = str(item.get("id") or "")
            if not source_id.endswith("_1m"):
                continue
            prefix = source_id.removesuffix("_1m")
            if prefix not in native_five_minute_prefixes:
                continue
            item["id"] = f"{prefix}_5m"
            item["grain"] = "instrument_native_5m_aligned_to_1m"
            item["history_contract"] = (
                "official native 5m observations within the provider retention "
                "boundary; causally aligned to completed 1m bars without interpolation"
            )
        return migrated

    specs = (
        (
            "okx",
            "OKX",
            root / "data_okx/1m/okx_historical_feature_catalog.json",
            root / "data_okx/okx_historical_feature_catalog.json",
            root / "data_okx/1m/historical_feature_report.csv",
        ),
        (
            "binance",
            "Binance",
            root / "data_binance/1m/binance_historical_feature_catalog.json",
            root / "data_binance/binance_historical_feature_catalog.json",
            root / "data_binance/1m/historical_feature_report.csv",
        ),
    )
    rows: list[dict[str, Any]] = []
    for dataset, provider, catalog_path, legacy_catalog_path, report_path in specs:
        payload = _read_json(catalog_path, {})
        if not payload:
            legacy = _read_json(legacy_catalog_path, {})
            payload = (
                migrate_legacy_catalog(legacy)
                if isinstance(legacy, Mapping) and legacy
                else {}
            )
        catalog = payload.get("catalog", []) if isinstance(payload, Mapping) else []
        if not isinstance(catalog, list):
            continue
        latest = _latest_time([catalog_path, report_path], [payload])
        fresh = _freshness(latest, now=now, window_seconds=6 * 3600)
        report_rows: list[dict[str, str]] = []
        try:
            with report_path.open(encoding="utf-8", newline="") as handle:
                report_rows = [dict(row) for row in csv.DictReader(handle)]
        except (FileNotFoundError, OSError, UnicodeError, csv.Error):
            pass
        stage_statuses: dict[str, list[str]] = {}
        stage_coverage: dict[str, int] = {}
        for report in report_rows:
            try:
                statuses = json.loads(report.get("stage_status_json") or "{}")
            except json.JSONDecodeError:
                statuses = {}
            try:
                coverage = json.loads(report.get("coverage_json") or "{}")
            except json.JSONDecodeError:
                coverage = {}
            if isinstance(statuses, Mapping):
                for stage, status in statuses.items():
                    stage_statuses.setdefault(str(stage), []).append(str(status))
            if isinstance(coverage, Mapping):
                for stage, item in coverage.items():
                    if isinstance(item, Mapping):
                        stage_coverage[str(stage)] = stage_coverage.get(
                            str(stage), 0
                        ) + (_integer(item.get("rows")) or 0)

        id_to_stage = {
            "mark_price_candles_1m": "mark_price",
            "index_price_candles_1m": "index_price",
            "premium_index_candles_1m": "premium_index",
            "funding_rate": "funding_rate",
            "funding_rate_history": "funding_rate",
            "open_interest_5m": "open_interest",
            "contract_taker_volume_5m": "taker_volume",
            "taker_buy_sell_volume_5m": "taker_buy_sell_volume",
            "contract_long_short_account_ratio_5m": "long_short_account_ratio",
            "global_long_short_account_ratio_5m": ("global_long_short_account_ratio"),
            "top_trader_account_ratio_5m": "top_trader_account_ratio",
            "top_trader_position_ratio_5m": "top_trader_position_ratio",
            "basis_5m": "basis",
        }
        for item in catalog:
            if not isinstance(item, Mapping):
                continue
            source_id = str(item.get("id") or "").strip()
            if not source_id:
                continue
            download_status = str(item.get("download_status") or "registered")
            stage = id_to_stage.get(source_id)
            statuses = stage_statuses.get(stage or "", [])
            completed = sum(status == "ok" for status in statuses)
            failures = sum(status == "failed" for status in statuses)
            total = len(statuses)
            scheduled = download_status.startswith("included")
            deferred = download_status.startswith(
                "separate"
            ) or download_status.startswith("excluded")
            if failures:
                status = "degraded"
                label = f"{failures:,} 個商品階段失敗"
                eta = _unknown_eta("waiting_schedule", "等待下一輪端點重試。")
            elif scheduled and total and completed == total:
                status = "current" if fresh["state"] == "current" else "stale"
                label = (
                    "已納入更新且最近批次成功"
                    if status == "current"
                    else "歷史已抓但需要增量"
                )
                eta = _complete_eta("最近逐商品階段均成功。")
            elif source_id == "trade_candles_1m":
                status = "current" if fresh["state"] == "current" else "stale"
                label = "由主 OHLCV 更新器維護"
                eta = _complete_eta("主價格資料由同一交易所批次維護。")
            elif deferred:
                status = "waiting"
                label = "已登錄，需獨立容量／時序管線"
                eta = _unknown_eta(
                    "waiting_schedule",
                    "來源免費但不是緊湊歷史端點；尚未有可量測中的工作批次。",
                )
            else:
                status = "waiting"
                label = "已登錄，等待首批回執"
                eta = _unknown_eta("waiting_schedule", "尚無逐商品吞吐率可估算。")
            rows.append(
                {
                    "id": f"{dataset}-feature:{source_id}",
                    "parent_id": f"group:{dataset}",
                    "scope": "logical_source",
                    "title": source_id,
                    "provider": provider,
                    "category": str(
                        item.get("category") or item.get("grain") or "feature"
                    ),
                    "status": status,
                    "status_label": label,
                    "cadence": "1 分鐘增量" if scheduled else "獨立排程",
                    "update_owner": f"{provider} 公開資料更新器",
                    "latest_at_utc": _iso(latest),
                    "data_through": None,
                    "freshness": dict(fresh),
                    "coverage": _coverage(
                        completed,
                        total,
                        unit="商品",
                        label="最近特徵階段",
                    ),
                    "eta": eta,
                    "rows": stage_coverage.get(stage or ""),
                    "publishable": True,
                    "automation_eligible": bool(scheduled)
                    or source_id == "trade_candles_1m",
                    "detail": str(
                        item.get("model_role")
                        or item.get("reason")
                        or item.get("history_contract")
                        or "交易所公開資料。"
                    ),
                    "warnings": (
                        ["目前快照不得倒填歷史；只能從 observed_at 之後使用。"]
                        if "snapshot" in download_status
                        else []
                    ),
                    "detail_link": None,
                }
            )
    return rows


def _credential_states(root: Path) -> dict[str, dict[str, Any]]:
    payload = _read_json(root / "artifacts/data_credentials/status.json", {})
    providers = payload.get("providers", []) if isinstance(payload, Mapping) else []
    return {
        str(item.get("id")): dict(item)
        for item in providers
        if isinstance(item, Mapping) and item.get("id")
    }


def _credential_registry_sources(
    root: Path, *, now: datetime
) -> list[dict[str, Any]]:
    """Expose credential readiness without ever exposing credential values."""

    payload = _read_json(root / "artifacts/data_credentials/status.json", {})
    providers = payload.get("providers", []) if isinstance(payload, Mapping) else []
    generated_at = (
        _parse_time(payload.get("generated_at_utc"))
        if isinstance(payload, Mapping)
        else None
    )
    freshness = _freshness(generated_at, now=now, window_seconds=26 * 3600)
    rows: list[dict[str, Any]] = []
    for item in providers if isinstance(providers, list) else []:
        if not isinstance(item, Mapping) or not item.get("id"):
            continue
        credential_id = str(item["id"])
        provider = str(item.get("provider") or credential_id)
        state = str(item.get("state") or "missing")
        configured = _integer(item.get("configured_count")) or 0
        required = max(1, _integer(item.get("required_count")) or 1)
        is_openbb = credential_id.startswith("openbb:")
        if state == "configured":
            status = "current"
            label = "所需憑證已安全設定"
            eta = _complete_eta("憑證存在性稽核已通過；未讀出或發布金鑰值。")
        else:
            status = "unavailable"
            label = f"憑證狀態：{state}（{configured}/{required}）"
            eta = _unknown_eta("blocked", "缺少必要憑證，相關端點不會啟動。")
        rows.append(
            {
                "id": f"credential:{credential_id}",
                "parent_id": (
                    "group:openbb-compact"
                    if is_openbb
                    else "group:tw-minute-source-cold"
                    if credential_id == "shioaji"
                    else "group:free-public-context"
                ),
                "scope": "credential_gate",
                "title": f"{provider} · API 憑證",
                "provider": provider,
                "category": "credential_readiness",
                "status": status,
                "status_label": label,
                "cadence": "每次資料更新前重新稽核",
                "update_owner": "非機密憑證存在性稽核",
                "latest_at_utc": _iso(generated_at),
                "data_through": None,
                "freshness": freshness,
                "coverage": _coverage(
                    configured,
                    required,
                    unit="必要欄位",
                    label="憑證完整度",
                ),
                "eta": eta,
                "rows": None,
                "publishable": True,
                "automation_eligible": False,
                "credential_state": state,
                "detail": (
                    "此列只呈現 configured/partial/missing 與欄位計數；"
                    "API key、secret、token 值永不進入公開 payload。"
                ),
                "warnings": ["憑證就緒不等於免費方案具有資料權限或足夠配額。"],
                "detail_link": None,
            }
        )
    return rows


def _product_granularity_sources(
    root: Path, *, now: datetime
) -> list[dict[str, Any]]:
    """Flatten the canonical daily/1m/tick contract into auditable rows."""

    registry = _read_json(root / "configs/data_product_granularities.json", {})
    products = registry.get("products", []) if isinstance(registry, Mapping) else []
    credentials = _credential_states(root)
    rows: list[dict[str, Any]] = []
    windows = {"daily": 72 * 3600, "1m": 6 * 3600, "tick": 10 * 60}
    for product in products:
        if not isinstance(product, Mapping):
            continue
        product_id = str(product.get("id") or "").strip()
        product_title = str(product.get("title") or product_id)
        provider = str(product.get("provider") or "其他")
        granularities = product.get("granularities", [])
        if not product_id or not isinstance(granularities, list):
            continue
        for spec in granularities:
            if not isinstance(spec, Mapping):
                continue
            grain = str(spec.get("granularity") or "").strip()
            if grain not in {"daily", "1m", "tick"}:
                continue
            implementation = str(spec.get("implementation") or "registered")
            availability = str(spec.get("availability") or "unspecified")
            parent = str(spec.get("parent_dataset") or "")
            storage_relative = str(spec.get("storage_path") or "")
            summary_relative = str(spec.get("summary_path") or "")
            progress_relative = str(spec.get("progress_path") or "")
            storage_path = root / storage_relative if storage_relative else None
            summary_path = root / summary_relative if summary_relative else None
            progress_path = root / progress_relative if progress_relative else None
            summary = _read_json(summary_path, {}) if summary_path else {}
            progress = _read_json(progress_path, {}) if progress_path else {}
            latest = _latest_time(
                [path for path in (summary_path, storage_path) if path is not None],
                [summary] if isinstance(summary, Mapping) else [],
            )
            data_through = (
                _extract_data_through([summary])
                if isinstance(summary, Mapping)
                else None
            )
            data_through_time = _parse_time(data_through)
            if data_through_time is not None:
                latest = data_through_time
            freshness = _freshness(
                latest,
                now=now,
                window_seconds=windows[grain],
            )
            counts = _status_counts([summary]) if isinstance(summary, Mapping) else {}
            total = sum(counts.values())
            failures = sum(
                count
                for status_name, count in counts.items()
                if any(
                    token in status_name.lower()
                    for token in ("fail", "error", "partial", "mismatch")
                )
            )
            completed = max(0, total - failures)
            coverage = (
                _coverage(completed, total, unit="項", label="最近批次")
                if total
                else _coverage(1, 1, unit="契約", label="資料路徑")
                if storage_path is not None and storage_path.exists()
                else _coverage(0, 1, unit="契約", label="資料路徑")
            )
            credential_id = str(spec.get("credential_id") or "")
            credential = credentials.get(credential_id) if credential_id else None
            credential_state = (
                str(credential.get("state") or "missing")
                if isinstance(credential, Mapping)
                else "unknown"
                if credential_id
                else "not_required"
            )
            implemented = implementation.startswith("implemented") or implementation.startswith(
                "reused_existing"
            )
            registered_only = implementation.startswith("registered")
            unsupported = implementation == "not_available"
            deferred = implementation.startswith("deferred") or spec.get(
                "acquisition_enabled"
            ) is False
            warnings: list[str] = []
            if deferred:
                status = "deferred"
                exchange_scope = "exchange_scope" in implementation
                status_label = (
                    "依目前交易所範圍延後"
                    if exchange_scope
                    else "依目前 1m-only 範圍延後"
                )
                eta = _complete_eta(
                    "目前只啟用 Binance、OKX、Bybit；此交易所不會消耗流量或容量。"
                    if exchange_scope
                    else "逐筆／委託簿資料未排程；不會消耗流量或容量。"
                )
                automation_eligible = False
                coverage = _coverage(1, 1, unit="範圍契約", label="延後狀態")
                warnings.append(
                    "既有資料保留；只有使用者明確擴大交易所範圍後才可續抓。"
                    if exchange_scope
                    else "既有逐筆資料保留；只有使用者明確重新啟用後才可續抓。"
                )
            elif credential_id and credential_state != "configured":
                status = "unavailable"
                status_label = f"憑證狀態：{credential_state}"
                eta = _unknown_eta("blocked", "所需 API 憑證尚未完整設定。")
                automation_eligible = False
                warnings.append("面板只顯示憑證是否存在，不公開任何 API key 值。")
            elif unsupported:
                status = "unavailable"
                status_label = "來源沒有可驗證的逐筆歷史契約"
                eta = _unknown_eta("blocked", "此供應商不提供該資料粒度。")
                automation_eligible = False
            elif registered_only:
                status = "waiting"
                status_label = "已註冊但尚未接入可執行管線"
                eta = _unknown_eta(
                    "waiting_schedule",
                    "需要來源權限、容量或 materializer 完成後才有可量測 ETA。",
                )
                automation_eligible = False
            elif failures:
                status = "degraded"
                status_label = f"最近批次有 {failures:,} 個失敗／部分完成項"
                eta = _unknown_eta("waiting_schedule", "等待下一輪精確重試。")
                automation_eligible = implemented
            elif implemented and latest is not None:
                status = "current" if freshness["state"] == "current" else "stale"
                status_label = "已到最新" if status == "current" else "需要補到最新"
                eta = (
                    _complete_eta("最近資料與摘要通過新鮮度檢查。")
                    if status == "current"
                    else _unknown_eta("waiting_schedule", "等待下一輪增量或回補。")
                )
                automation_eligible = True
            elif implemented:
                status = "waiting"
                status_label = "管線已實作，等待第一份可驗證回執"
                eta = _unknown_eta("waiting_schedule", "首次批次尚未產生吞吐率。")
                automation_eligible = True
            else:
                status = "unavailable"
                status_label = "未知的實作契約"
                eta = _unknown_eta("blocked", "註冊資料不完整。")
                automation_eligible = False

            if not deferred and isinstance(progress, Mapping) and progress:
                progress_updated = _parse_time(progress.get("updated_at_utc"))
                progress_age = (
                    max(0.0, (now - progress_updated).total_seconds())
                    if progress_updated is not None
                    else None
                )
                if (
                    str(progress.get("state") or "").lower() == "running"
                    and progress_age is not None
                    and progress_age <= 15 * 60
                ):
                    status = "updating"
                    status_label = str(progress.get("label") or "資料更新正在執行")
                    coverage = _coverage(
                        progress.get("current"),
                        progress.get("total"),
                        unit=str(progress.get("unit") or "項"),
                        label="目前批次",
                    )
                    remaining = _integer(progress.get("remaining_seconds"))
                    eta = {
                        "state": "estimating" if remaining is not None else "warming_up",
                        "remaining_seconds": remaining,
                        "estimated_complete_at_utc": progress.get(
                            "estimated_complete_at_utc"
                        ),
                        "confidence": "low" if remaining is not None else "not_available",
                        "basis": str(
                            progress.get("basis")
                            or "依完整批次的實測吞吐率線性外推。"
                        ),
                    }
                    latest = progress_updated
            if grain == "tick":
                warnings.append("Tick 必須是真實成交／報價事件；不以 1 分鐘 K 線冒充。")
            if "rolling" in availability or "recent" in availability:
                warnings.append("來源歷史範圍有限；完整度只在官方可取得邊界內成立。")
            rows.append(
                {
                    "id": f"product:{product_id}:{grain}",
                    "parent_id": f"group:{parent}" if parent else None,
                    "scope": "product_granularity",
                    "title": f"{product_title} · {grain}",
                    "provider": provider,
                    "category": "market_product_granularity",
                    "granularity": grain,
                    "native_granularity": grain,
                    "availability": availability,
                    "implementation": implementation,
                    "credential_state": credential_state,
                    "status": status,
                    "status_label": status_label,
                    "cadence": str(spec.get("cadence") or "依來源排程"),
                    "update_owner": f"{provider} {grain} 管線",
                    "latest_at_utc": _iso(latest),
                    "data_through": data_through,
                    "freshness": freshness,
                    "coverage": coverage,
                    "eta": eta,
                    "rows": _extract_rows([summary])
                    if isinstance(summary, Mapping)
                    else None,
                    "publishable": True,
                    "automation_eligible": automation_eligible,
                    "acquisition_enabled": not deferred,
                    "stream_contract": bool(spec.get("stream")),
                    "detail": (
                        f"availability={availability}; implementation={implementation}; "
                        f"storage={storage_relative or 'none'}"
                    ),
                    "warnings": warnings,
                    "detail_link": None,
                }
            )
    return rows


def _crypto_acquisition_sources(
    root: Path, *, now: datetime
) -> list[dict[str, Any]]:
    """Expose the first-principles crypto fact registry and its unique owner."""

    registry = _read_json(root / "configs/crypto_data_acquisition.json", {})
    facts = registry.get("datasets", []) if isinstance(registry, Mapping) else []
    source_status = _read_json(root / "data_crypto_reference/source_status.json", {})
    provider_rows = (
        source_status.get("providers", [])
        if isinstance(source_status, Mapping)
        else []
    )
    dataset_rows = (
        source_status.get("datasets", [])
        if isinstance(source_status, Mapping)
        else []
    )
    free_manifest = _read_json(root / "data_free_public/download_manifest.json", {})
    free_dataset_rows = (
        free_manifest.get("results", [])
        if isinstance(free_manifest, Mapping)
        else []
    )
    providers = {
        str(item.get("credential_id")): item
        for item in provider_rows
        if isinstance(item, Mapping) and item.get("credential_id")
    }
    evidence = {
        str(item.get("dataset")): item
        for item in [*dataset_rows, *free_dataset_rows]
        if isinstance(item, Mapping) and item.get("dataset")
    }
    fact_evidence_ids = {
        "aggregate_asset_identity": ["coingecko_asset_catalog"],
        "aggregate_market_cap_supply": ["coingecko_market_snapshot"],
        "ethereum_realtime_gas": [
            "blockscout_ethereum_gas",
            "blockscout_ethereum_latest_block",
        ],
        "stablecoin_supply_and_peg": ["defillama_stablecoins"],
        "defi_tvl": ["defillama_chains"],
        "dex_derivatives_options_volume": [
            "defillama_dex_volume",
            "defillama_options_notional_volume",
            "defillama_open_interest",
        ],
        "protocol_fees_revenue": [
            "defillama_protocol_fees",
            "defillama_protocol_revenue",
        ],
        "defi_yields_and_borrow_rates": ["defillama_yields"],
        "bitcoin_mempool_and_fees": [
            "bitcoin_mempool_fees",
            "bitcoin_mempool_state",
            "bitcoin_difficulty_adjustment",
            "bitcoin_hashrate_history",
        ],
        "sentiment_indices": ["alternative_me_fear_greed"],
        "options_chain_greeks_iv": [
            "deribit_btc_options",
            "deribit_eth_options",
        ],
    }
    fact_progress_paths = {
        "venue_instrument_lifecycle": root / "data_binance_archive/progress.json",
        "venue_spot_ohlcv_1m": root / "data_binance_archive/progress.json",
        "venue_dated_futures_ohlcv_1m": root
        / "data_binance_archive/progress.json",
    }
    provider_paths = {
        "Binance": [
            root / "data_binance/1m/download_summary.json",
            root / "data_binance/download_summary.json",
            root / "data_binance/progress.json",
            root / "data_binance_archive/download_summary.json",
            root / "data_binance_archive/plan_summary.json",
            root / "data_binance_archive/capacity_receipt.json",
            root / "data_binance_archive/progress.json",
        ],
        "OKX": [
            root / "data_okx/1m/download_summary.json",
            root / "data_okx/download_summary.json",
            root / "data_okx/progress.json",
        ],
        "Bybit": [
            root / "data_bybit/1m/download_summary.json",
            root / "data_bybit/download_summary.json",
            root / "data_bybit/progress.json",
        ],
        "DefiLlama": [
            root / "data_free_public/download_summary.json",
            root / "data_free_public/download_manifest.json",
        ],
        "Coin Metrics": [root / "data_coinmetrics_community/download_summary.json"],
        "mempool.space": [root / "data_free_public/download_manifest.json"],
        "Deribit": [root / "data_free_public/download_manifest.json"],
        "Alternative.me": [root / "data_free_public/download_manifest.json"],
        "Blockscout": [root / "data_free_public/download_manifest.json"],
        "OpenBB": [root / "data_openBB/_state/monitor_latest.json"],
    }
    output: list[dict[str, Any]] = []
    for fact in facts if isinstance(facts, list) else []:
        if not isinstance(fact, Mapping) or not fact.get("id"):
            continue
        fact_id = str(fact["id"])
        title = str(fact.get("title") or fact_id)
        implementation = str(fact.get("implementation") or "registered_pending")
        priority = str(fact.get("priority") or "P2")
        score = _integer(fact.get("score")) or 0
        owners = [
            str(item)
            for item in fact.get("canonical_owners", [])
            if str(item).strip()
        ]
        fallbacks = [
            str(item) for item in fact.get("fallbacks", []) if str(item).strip()
        ]
        credential_id = str(fact.get("credential_id") or "")
        provider_state = providers.get(credential_id) if credential_id else None
        expected_evidence_ids = fact_evidence_ids.get(fact_id, [])
        direct_items = [
            evidence[dataset_id]
            for dataset_id in expected_evidence_ids
            if dataset_id in evidence
        ]
        evidence_current = sum(
            str(item.get("status")) in {"updated", "current_cached"}
            for item in direct_items
        )
        evidence_paths = [
            path
            for owner in owners
            for token, paths in provider_paths.items()
            if token.lower() in owner.lower()
            for path in paths
        ]
        latest = _latest_time(
            evidence_paths,
            [],
        )
        direct_times = [
            _parse_time(item.get("observed_at_utc"))
            for item in direct_items
            if isinstance(item, Mapping)
        ]
        direct_times = [value for value in direct_times if value is not None]
        if direct_times:
            latest = max(direct_times)
        window = 26 * 3600 if "snapshot" in str(fact.get("native_granularity")) else 72 * 3600
        fresh = _freshness(latest, now=now, window_seconds=window)
        coverage = (
            _coverage(
                evidence_current,
                len(expected_evidence_ids),
                unit="資料集",
                label="唯一主來源落盤",
            )
            if expected_evidence_ids
            else None
        )
        rows = (
            sum(_integer(item.get("rows") or item.get("observations_added")) or 0 for item in direct_items)
            if direct_items
            else None
        )
        status: str
        status_label: str
        eta: dict[str, Any]
        automation_eligible = False
        operational = (
            str(provider_state.get("operational_state") or "")
            if isinstance(provider_state, Mapping)
            else ""
        )
        blocked_provider = operational in {
            "invalid_credential",
            "not_entitled",
            "quota_exhausted",
            "unavailable",
        }
        evidence_complete = bool(expected_evidence_ids) and evidence_current == len(
            expected_evidence_ids
        )
        deferred = implementation.startswith("deferred") or fact.get(
            "acquisition_enabled"
        ) is False
        if deferred:
            status = "deferred"
            status_label = "依目前 1m-only 範圍延後"
            eta = _complete_eta("不啟動逐筆、報價簿、L2/L3 或強平事件取得。")
            automation_eligible = False
        elif evidence_complete and implementation.startswith("implemented") and not any(
            token in implementation
            for token in ("partial", "only", "pending", "requires", "blocked")
        ):
            status = "current" if fresh["state"] == "current" else "stale"
            status_label = "唯一主來源已落盤" if status == "current" else "唯一主來源需要更新"
            eta = (
                _complete_eta("最近唯一主來源回執已成功。")
                if status == "current"
                else _unknown_eta("waiting_schedule", "等待 cadence receipt 到期後補到最新。")
            )
            automation_eligible = True
        elif (
            blocked_provider
            and fact_id != "venue_liquidations"
            and not (implementation.startswith("implemented") and latest is not None)
        ):
            status = "unavailable"
            status_label = f"設定 API 不可用：{operational}"
            eta = _unknown_eta("blocked", "憑證存在不代表有效或具有端點權限。")
        elif implementation.startswith("blocked"):
            status = "unavailable"
            status_label = "已驗證阻擋條件"
            eta = _unknown_eta("blocked", "必須先修復金鑰或取得免費可用替代來源。")
        elif implementation.startswith("reused_existing"):
            status = "current" if latest is not None else "waiting"
            status_label = "沿用既有可稽核管線" if latest is not None else "等待既有管線回執"
            eta = (
                _complete_eta("既有專用資料管線負責更新。")
                if latest is not None
                else _unknown_eta("waiting_schedule", "尚未找到對應回執。")
            )
            automation_eligible = True
        elif implementation.startswith("implemented"):
            partial = any(
                token in implementation
                for token in ("partial", "only", "pending", "requires", "blocked")
            )
            if latest is None:
                status = "waiting"
                status_label = "已實作，等待第一份可驗證回執"
                eta = _unknown_eta("waiting_schedule", "首批吞吐率尚不可量測。")
            elif partial:
                status = "degraded"
                status_label = "部分場館／歷史已實作，剩餘缺口仍在清冊"
                eta = _unknown_eta("waiting_schedule", "不同場館與歷史邊界需分別完成。")
            else:
                status = "current" if fresh["state"] == "current" else "stale"
                status_label = "已由唯一主來源維護" if status == "current" else "需要補到最新"
                eta = (
                    _complete_eta("最近來源回執在允許的新鮮度內。")
                    if status == "current"
                    else _unknown_eta("waiting_schedule", "等待下一輪更新。")
                )
            automation_eligible = True
        else:
            status = "waiting"
            status_label = "價值與主來源已確定，下載器尚待完成"
            eta = _unknown_eta(
                "waiting_schedule",
                "未執行的工作不以零吞吐率捏造 ETA。",
            )
        fact_progress_path = fact_progress_paths.get(fact_id)
        fact_progress = (
            _read_json(fact_progress_path, {})
            if fact_progress_path is not None
            else {}
        )
        if not deferred and isinstance(fact_progress, Mapping) and str(
            fact_progress.get("state") or ""
        ).lower() == "running":
            progress_updated = _parse_time(fact_progress.get("updated_at_utc"))
            progress_age = (
                max(0.0, (now - progress_updated).total_seconds())
                if progress_updated is not None
                else None
            )
            if progress_age is not None and progress_age <= 15 * 60:
                remaining = _integer(fact_progress.get("remaining_seconds"))
                progress_phase = str(fact_progress.get("phase") or "")
                status = "updating"
                status_label = (
                    "唯一主來源正在建立精確容量計畫"
                    if progress_phase == "discover"
                    else "唯一主來源正在回補"
                )
                eta = {
                    "state": "estimating" if remaining is not None else "warming_up",
                    "remaining_seconds": remaining,
                    "estimated_complete_at_utc": fact_progress.get(
                        "estimated_complete_at_utc"
                    ),
                    "confidence": "low" if remaining is not None else "not_available",
                    "basis": (
                        "僅為官方目錄規劃階段 ETA；完整下載 ETA 尚未可量測。"
                        if progress_phase == "discover"
                        else str(
                            fact_progress.get("basis")
                            or "依目前完成吞吐率估計。"
                        )
                    ),
                    "phase": progress_phase or None,
                }
                automation_eligible = True
        owner_text = " / ".join(owners) if owners else "待指定"
        detail = (
            f"{fact.get('mechanism') or ''} primary={owner_text}; "
            f"fallback={' / '.join(fallbacks) if fallbacks else 'none'}"
        )
        warnings = [
            f"去重鍵：{' + '.join(str(item) for item in fact.get('dedup_key', []))}",
            "不同交易所的同一幣種是不同市場，不互相去重。",
        ]
        if deferred:
            warnings.append(
                str(fact.get("deferred_reason") or "依使用者決策延後；既有資料不刪除。")
            )
        if blocked_provider:
            warnings.append(
                str(provider_state.get("message") or operational)
                if isinstance(provider_state, Mapping)
                else operational
            )
        output.append(
            {
                "id": f"crypto-fact:{fact_id}",
                "parent_id": "group:crypto-reference",
                "scope": "crypto_fact_family",
                "title": f"{priority} · {title}",
                "provider": owner_text,
                "category": "crypto_canonical_fact",
                "availability": f"priority={priority}; value_score={score}/10; implementation={implementation}",
                "status": status,
                "status_label": status_label,
                "cadence": str(fact.get("native_granularity") or "依來源事件"),
                "update_owner": f"唯一主來源：{owner_text}",
                "latest_at_utc": _iso(latest),
                "data_through": None,
                "freshness": fresh,
                "coverage": coverage,
                "eta": eta,
                "rows": rows,
                "publishable": True,
                "automation_eligible": automation_eligible,
                "acquisition_enabled": not deferred,
                "priority": priority,
                "value_score": score,
                "dedup_key": list(fact.get("dedup_key", [])),
                "credential_state": (
                    str(provider_state.get("credential_state"))
                    if isinstance(provider_state, Mapping)
                    else "not_required"
                ),
                "credential_operational_state": operational or "not_required",
                "detail": detail,
                "warnings": warnings,
                "detail_link": None,
            }
        )
    return output


def _free_public_registry_sources(root: Path, *, now: datetime) -> list[dict[str, Any]]:
    registry = _read_json(root / "configs/free_public_data_sources.json", {})
    sources = registry.get("sources", []) if isinstance(registry, Mapping) else []
    manifest = _read_json(root / "data_free_public/download_manifest.json", {})
    manifest_results = (
        manifest.get("results", []) if isinstance(manifest, Mapping) else []
    )
    by_dataset = {
        str(item.get("dataset")): item
        for item in manifest_results
        if isinstance(item, Mapping) and item.get("dataset")
    }
    grouped_result_ids = {
        "hyperliquid_public_context": tuple(
            key for key in by_dataset if key.startswith("hyperliquid_")
        ),
        "deribit_public_context": tuple(
            key for key in by_dataset if key.startswith("deribit_")
        ),
    }
    output: list[dict[str, Any]] = []
    for source in sources if isinstance(sources, list) else []:
        if not isinstance(source, Mapping):
            continue
        source_id = str(source.get("id") or "").strip()
        if not source_id:
            continue
        implementation = str(source.get("implementation_status") or "registered")
        configured_result_ids = source.get("dataset_ids")
        if isinstance(configured_result_ids, list) and configured_result_ids:
            result_ids = tuple(
                str(item).strip() for item in configured_result_ids if str(item).strip()
            )
        else:
            result_ids = grouped_result_ids.get(source_id, (source_id,))
        evidence = [by_dataset[key] for key in result_ids if key in by_dataset]
        summary_path = source.get("summary_path")
        if not evidence and isinstance(summary_path, str) and summary_path.strip():
            external_summary = _read_json(root / summary_path, {})
            external_status_counts = (
                external_summary.get("status_counts", {})
                if isinstance(external_summary, Mapping)
                else {}
            )
            external_rows = _integer(external_summary.get("row_count"))
            if external_rows is not None and external_rows > 0:
                external_failed = sum(
                    _integer(external_status_counts.get(key)) or 0
                    for key in ("failed", "error")
                )
                evidence = [
                    {
                        "dataset": source_id,
                        "status": "failed" if external_failed else "updated",
                        "observations_added": external_rows,
                        "observed_at_utc": external_summary.get("ended_at_utc"),
                    }
                ]
                result_ids = (source_id,)
        failures = sum(str(item.get("status")) == "failed" for item in evidence)
        completed = sum(str(item.get("status")) == "updated" for item in evidence)
        expected = len(result_ids) if result_ids else 1
        latest = max(
            (
                parsed
                for item in evidence
                if (parsed := _parse_time(item.get("observed_at_utc"))) is not None
            ),
            default=None,
        )
        fresh = _freshness(latest, now=now, window_seconds=26 * 3600)
        explicitly_deferred = implementation.startswith("deferred_by_user")
        if explicitly_deferred:
            status = "deferred"
            label = "依目前交易所範圍暫停"
            eta = _unknown_eta(
                "waiting_schedule",
                "目前只啟用 Binance、OKX、Bybit；既有檔案保留但不再自動抓取。",
            )
        elif failures:
            status = "degraded"
            label = f"最近捕捉有 {failures:,} 個資料集失敗"
            eta = _unknown_eta("waiting_schedule", "等待下一輪匿名端點重試。")
        elif evidence and completed == len(evidence):
            status = "current" if fresh["state"] == "current" else "stale"
            label = (
                "已取得且保留觀測版本" if status == "current" else "已取得但需要新快照"
            )
            eta = _complete_eta("最近已登錄的緊湊資料集皆成功落盤。")
        elif implementation.startswith("reused_existing"):
            status = "current"
            label = "由既有專用下載器維護"
            eta = _complete_eta("狀態與完整度由對應專用面板呈現。")
        elif any(
            token in implementation for token in ("pending", "gate", "next_backfill")
        ):
            status = "waiting"
            label = "已登錄，等待下載／容量／憑證條件"
            eta = _unknown_eta(
                "waiting_schedule",
                "尚未有執行中批次，不能以零吞吐率捏造 ETA。",
            )
        else:
            status = "waiting"
            label = "已登錄，等待首批可驗證回執"
            eta = _unknown_eta("waiting_schedule", "尚無批次速率可估算。")
        dataset_group = str(source.get("dataset_group") or "free-public-context")
        parent_id = (
            f"group:{dataset_group}"
            if dataset_group in _GROUP_META
            else "group:free-public-context"
        )
        output.append(
            {
                "id": f"free-source:{source_id}",
                "parent_id": parent_id,
                "scope": "source_registry",
                "title": source_id,
                "provider": str(source.get("provider") or "公開來源"),
                "category": str(source.get("category") or "market-context"),
                "status": status,
                "status_label": label,
                "cadence": str(source.get("cadence") or "依來源排程"),
                "update_owner": "來源清冊／對應專用下載器",
                "latest_at_utc": _iso(latest),
                "data_through": None,
                "freshness": fresh,
                "coverage": _coverage(
                    completed,
                    expected,
                    unit="資料集",
                    label="最近匿名捕捉",
                ),
                "eta": eta,
                "rows": sum(
                    _integer(item.get("observations_added")) or 0 for item in evidence
                )
                or None,
                "publishable": True,
                "automation_eligible": not explicitly_deferred and not any(
                    token in implementation
                    for token in ("pending", "gate", "next_backfill")
                ),
                "acquisition_enabled": not explicitly_deferred,
                "detail": str(
                    source.get("history_contract")
                    or source.get("feature_status")
                    or "已登錄免費公開來源。"
                ),
                "warnings": (
                    [str(source.get("feature_status"))]
                    if source.get("feature_status")
                    else []
                ),
                "detail_link": None,
            }
        )
    return output


def _calendar_partition_count(start_text: str, end: date, chunk_months: int) -> int:
    try:
        cursor = date.fromisoformat(start_text)
    except ValueError:
        return 0
    count = 0
    while cursor < end:
        absolute = cursor.year * 12 + cursor.month - 1 + max(1, chunk_months)
        year, month0 = divmod(absolute, 12)
        cursor = min(date(year, month0 + 1, 1), end)
        count += 1
    return count


def _crypto_history_sources(root: Path, *, now: datetime) -> list[dict[str, Any]]:
    """Expose every registered Dune query and issuer endpoint with receipt evidence."""

    output: list[dict[str, Any]] = []
    dune_config = _read_json(root / "configs/dune_crypto_queries.json", {})
    dune_summary = _read_json(root / "data_dune_crypto/download_summary.json", {})
    dune_progress = _read_json(root / "data_dune_crypto/progress.json", {})
    dune_results: dict[str, list[Mapping[str, Any]]] = {}
    if isinstance(dune_summary, Mapping):
        for item in dune_summary.get("results", []):
            if not isinstance(item, Mapping):
                continue
            dune_results.setdefault(str(item.get("query_id")), []).append(item)
    dune_run_blocked = (
        isinstance(dune_summary, Mapping)
        and (
            str(dune_summary.get("state") or "") == "blocked"
            or (_integer(dune_summary.get("blocked_credit_partitions")) or 0) > 0
        )
    )
    progress_updated = _parse_time(dune_progress.get("updated_at_utc")) if isinstance(dune_progress, Mapping) else None
    progress_live = (
        isinstance(dune_progress, Mapping)
        and str(dune_progress.get("state") or "") == "running"
        and progress_updated is not None
        and (now - progress_updated).total_seconds() <= 15 * 60
    )
    for query in dune_config.get("queries", []) if isinstance(dune_config, Mapping) else []:
        if not isinstance(query, Mapping) or not query.get("enabled"):
            continue
        query_id = str(query.get("id") or "")
        receipts = sorted((root / "data_dune_crypto/receipts" / query_id).glob("*.json"))
        receipt_payloads = [_read_json(path, {}) for path in receipts]
        completed = sum(
            isinstance(item, Mapping) and item.get("status") == "complete"
            for item in receipt_payloads
        )
        expected = _calendar_partition_count(
            str(query.get("history_start") or ""),
            now.date(),
            int(query.get("chunk_months") or 3),
        )
        latest = _latest_time(receipts, receipt_payloads)
        query_results = dune_results.get(query_id, [])
        query_statuses = {str(item.get("status") or "") for item in query_results}
        result_status = next(
            (
                status
                for status in ("blocked_credits", "failed", "not_started", "complete")
                if status in query_statuses
            ),
            "",
        )
        if dune_run_blocked and query_statuses & {"blocked_credits", "not_started"}:
            result_status = "blocked_credits"
        query_progress_live = progress_live and query_id in str(
            dune_progress.get("phase") or ""
        )
        fresh = _freshness(latest, now=now, window_seconds=72 * 3600)
        if result_status == "blocked_credits":
            status, label = "blocked", "Dune credits 不足，已停止新增執行"
            eta = _unknown_eta("waiting_quota", "HTTP 402 後 fail-closed；等待 credits 恢復。")
        elif result_status in {"failed", "not_started"}:
            status, label = "degraded", "最近分區失敗／尚未啟動"
            eta = _unknown_eta("waiting_schedule", "完成分區保留，下一輪只重試缺口。")
        elif query_progress_live:
            status, label = "updating", "Dune 歷史分區正在回補"
            remaining = _integer(dune_progress.get("remaining_seconds"))
            eta = {
                "state": "estimating" if remaining is not None else "warming_up",
                "remaining_seconds": remaining,
                "estimated_complete_at_utc": dune_progress.get("estimated_complete_at_utc"),
                "confidence": "low" if remaining is not None else "not_available",
                "basis": str(dune_progress.get("basis") or "依已完成分區吞吐外推。"),
            }
        elif expected and completed >= expected and fresh["state"] == "current":
            status, label = "current", "歷史完整且已到最新分區"
            eta = _complete_eta("每個非重疊日曆分區都有完整回執。")
        elif completed:
            status, label = "waiting", f"已完成 {completed:,}/{expected:,} 個分區"
            eta = _unknown_eta("waiting_schedule", "等待每日回補器續跑；未執行時不捏造 ETA。")
        else:
            status, label = "waiting", "已註冊，等待第一個完整分區"
            eta = _unknown_eta("waiting_schedule", "尚無完整分區吞吐率。")
        output.append(
            {
                "id": f"dune-query:{query_id}",
                "parent_id": "group:dune-crypto",
                "scope": "registered_endpoint",
                "title": query_id,
                "provider": "Dune",
                "category": str(query.get("fact_family") or "onchain"),
                "status": status,
                "status_label": label,
                "cadence": "每日；每批 " + str(query.get("chunk_months") or 3) + " 個月",
                "update_owner": "Dune 版本化 SQL 回補器",
                "latest_at_utc": _iso(latest),
                "data_through": max(
                    (str(item.get("window_end_exclusive")) for item in receipt_payloads if isinstance(item, Mapping)),
                    default=None,
                ),
                "freshness": fresh,
                "coverage": _coverage(completed, expected, unit="分區", label="歷史分區") if expected else None,
                "eta": eta,
                "rows": sum(_integer(item.get("rows")) or 0 for item in receipt_payloads if isinstance(item, Mapping)),
                "publishable": False,
                "automation_eligible": True,
                "acquisition_enabled": True,
                "detail": str(query.get("description") or "已註冊 Dune SQL 查詢。"),
                "warnings": [
                    "原始 SQL SHA、execution_id、查詢視窗與可用時間均會落盤。",
                    "鏈上地址標籤流量不等於交易所儲備或償付能力證明。",
                ],
                "detail_link": None,
            }
        )

    etf_config = _read_json(root / "configs/crypto_etf_sources.json", {})
    etf_summary = _read_json(root / "data_crypto_etf/download_summary.json", {})
    etf_results = {
        str(item.get("source_id")): item
        for item in etf_summary.get("results", [])
        if isinstance(item, Mapping)
    } if isinstance(etf_summary, Mapping) else {}
    etf_latest = _parse_time(etf_summary.get("generated_at_utc")) if isinstance(etf_summary, Mapping) else None
    etf_fresh = _freshness(etf_latest, now=now, window_seconds=72 * 3600)
    sec_results = [
        item
        for key, item in etf_results.items()
        if key.startswith("sec_cik_")
    ]
    sec_missing = [
        item
        for key, item in etf_results.items()
        if key.startswith("sec_ticker_")
        and str(item.get("status") or "") == "unavailable_mapping"
    ]
    blocked_sec = etf_results.get("sec_edgar")
    sec_total = (
        (_integer(etf_summary.get("sec_entities")) or len(sec_results))
        + len(sec_missing)
        if isinstance(etf_summary, Mapping)
        else len(sec_results) + len(sec_missing)
    )
    sec_complete = sum(str(item.get("status")) == "complete" for item in sec_results)
    if isinstance(blocked_sec, Mapping):
        sec_status, sec_label = "blocked", "SEC_USER_AGENT 尚未設定"
        sec_eta = _unknown_eta("blocked", str(blocked_sec.get("message") or "SEC fair-access identification is required."))
    elif sec_missing:
        sec_status, sec_label = "degraded", f"{len(sec_missing):,} 個 ticker 缺少目前 SEC CIK 映射"
        sec_eta = _unknown_eta("waiting_schedule", "保留缺口，不以錯誤 CIK 或同名公司替代。")
    elif any(str(item.get("status")) in {"failed", "degraded"} for item in sec_results):
        sec_status, sec_label = "degraded", "部分 SEC 實體或原始申報文件失敗"
        sec_eta = _unknown_eta("waiting_schedule", "下一輪會沿用已完成檔案並只補缺口。")
    elif sec_results and sec_complete >= sec_total:
        sec_status = "current" if etf_fresh["state"] == "current" else "stale"
        sec_label = "所有已解析 CIK 已完成" if sec_status == "current" else "歷史完整但需要增量更新"
        sec_eta = _complete_eta("SEC submissions、companyfacts 與選定 primary documents 已落盤。")
    else:
        sec_status, sec_label = "waiting", "已註冊，等待 SEC 回填"
        sec_eta = _unknown_eta("waiting_schedule", "尚無完整 SEC 實體吞吐率。")
    output.append(
        {
            "id": "crypto-etf:sec-edgar",
            "parent_id": "group:crypto-etf-history",
            "scope": "registered_endpoint",
            "title": "SEC EDGAR submissions / companyfacts / primary filings",
            "provider": "U.S. SEC",
            "category": "regulatory_filings",
            "status": sec_status,
            "status_label": sec_label,
            "cadence": "每日與 filing event",
            "update_owner": "Crypto ETF SEC 回補器",
            "latest_at_utc": _iso(etf_latest),
            "data_through": None,
            "freshness": etf_fresh,
            "coverage": _coverage(sec_complete, sec_total, unit="CIK", label="SEC 實體") if sec_total else None,
            "eta": sec_eta,
            "rows": sum(_integer(item.get("rows")) or 0 for item in sec_results),
            "publishable": False,
            "automation_eligible": not isinstance(blocked_sec, Mapping),
            "acquisition_enabled": True,
            "detail": "每次以 SEC company_tickers.json 重新解析 ticker 到 CIK，再抓 submissions 全歷史分片、companyfacts 與選定原始申報文件。",
            "warnings": ["SEC 不需 API key，但公平存取政策要求可識別的 SEC_USER_AGENT。"],
            "detail_link": None,
        }
    )
    for spec in etf_config.get("issuer_sources", []) if isinstance(etf_config, Mapping) else []:
        if not isinstance(spec, Mapping):
            continue
        source_id = str(spec.get("id") or "")
        result = etf_results.get(source_id)
        receipt = _read_json(root / "data_crypto_etf/receipts/issuers" / f"{source_id}.json", {})
        latest = _parse_time(receipt.get("observed_at_utc")) if isinstance(receipt, Mapping) else None
        fresh = _freshness(latest, now=now, window_seconds=None if spec.get("immutable") else 72 * 3600)
        raw_status = str(result.get("status") or "") if isinstance(result, Mapping) else ""
        if raw_status == "failed":
            status, label = "degraded", "最近擷取或正規化失敗"
            eta = _unknown_eta("waiting_schedule", "等待每日排程重試。")
        elif receipt:
            status = "current" if spec.get("immutable") or fresh["state"] == "current" else "stale"
            label = "官方歷史檔已版本化" if spec.get("immutable") else ("官方日檔已更新" if status == "current" else "需要新日檔")
            eta = _complete_eta("原始 bytes、SHA-256 與正規化結果均已落盤。")
        else:
            status, label = "waiting", "已註冊，等待第一份官方檔案"
            eta = _unknown_eta("waiting_schedule", "尚無第一次成功回執。")
        output.append(
            {
                "id": f"crypto-etf:issuer:{source_id}",
                "parent_id": "group:crypto-etf-history",
                "scope": "registered_endpoint",
                "title": f"{spec.get('ticker')} · {source_id}",
                "provider": str(spec.get("provider") or "ETF issuer"),
                "category": str(spec.get("adapter") or "issuer_history"),
                "status": status,
                "status_label": label,
                "cadence": "不可變歷史檔" if spec.get("immutable") else "每日官方更新",
                "update_owner": "ETF 發行商歷史回補器",
                "latest_at_utc": _iso(latest),
                "data_through": None,
                "freshness": fresh,
                "coverage": _coverage(1 if receipt else 0, 1, unit="端點", label="官方來源回執"),
                "eta": eta,
                "rows": _integer(result.get("rows")) if isinstance(result, Mapping) else None,
                "publishable": False,
                "automation_eligible": True,
                "acquisition_enabled": True,
                "detail": str(spec.get("url") or "官方發行商資料來源"),
                "warnings": ["歷史列在本機首次觀測前不會被回填成 point-in-time 可用。"],
                "detail_link": None,
            }
        )
    return output


def _rollup_crypto_history_groups(
    groups: list[dict[str, Any]], logical: list[dict[str, Any]]
) -> None:
    """Keep storage-group cards no more optimistic than their endpoint rows."""

    for group_id in ("group:dune-crypto", "group:crypto-etf-history"):
        group = next((item for item in groups if item.get("id") == group_id), None)
        children = [item for item in logical if item.get("parent_id") == group_id]
        if group is None or not children:
            continue
        statuses = [str(item.get("status") or "unavailable") for item in children]
        if "blocked" in statuses or "unavailable" in statuses:
            group["status"] = "blocked"
            group["status_label"] = "至少一個必要端點無法完成"
        elif "degraded" in statuses:
            group["status"] = "degraded"
            group["status_label"] = "至少一個端點最近失敗"
        elif "updating" in statuses:
            group["status"] = "updating"
            group["status_label"] = "歷史端點正在回補"
        elif any(value in {"waiting", "stale"} for value in statuses):
            group["status"] = "waiting"
            group["status_label"] = "仍有歷史端點尚未補到最新"
        else:
            group["status"] = "current"
            group["status_label"] = "所有必要端點已到最新"
        latest_values = [
            parsed
            for item in children
            if (parsed := _parse_time(item.get("latest_at_utc"))) is not None
        ]
        if latest_values:
            group["latest_at_utc"] = _iso(max(latest_values))
        group["rows"] = sum(_integer(item.get("rows")) or 0 for item in children) or None
        if group_id == "group:dune-crypto":
            coverages = [
                item.get("coverage")
                for item in children
                if isinstance(item.get("coverage"), Mapping)
            ]
            current = sum(_integer(item.get("current")) or 0 for item in coverages)
            total = sum(_integer(item.get("total")) or 0 for item in coverages)
            group["coverage"] = _coverage(
                current, total, unit="分區", label="全部 Dune 歷史分區"
            )
        if group["status"] == "current":
            group["eta"] = _complete_eta("所有必要端點皆有完整且最新的回執。")
        elif group["status"] == "blocked":
            group["eta"] = _unknown_eta(
                "blocked", "至少一個必要端點仍被設定或來源條件阻擋。"
            )
        else:
            active_etas = [
                item.get("eta")
                for item in children
                if isinstance(item.get("eta"), Mapping)
                and str(item["eta"].get("state") or "")
                in {"estimating", "warming_up", "running_unmeasured"}
            ]
            measured = [
                item
                for item in active_etas
                if _integer(item.get("remaining_seconds")) is not None
            ]
            group["eta"] = (
                max(measured, key=lambda item: _integer(item.get("remaining_seconds")) or 0)
                if measured
                else dict(active_etas[0])
                if active_etas
                else _unknown_eta(
                    "blocked" if group["status"] == "blocked" else "waiting_schedule",
                    "等待必要端點解除阻擋或下一輪產生有效吞吐率。",
                )
            )


def _specialize_groups(
    groups: list[dict[str, Any]],
    *,
    shioaji: Mapping[str, Any],
    openbb: Mapping[str, Any],
    root: Path,
    now: datetime,
    refresh_services: Mapping[str, Mapping[str, Any]],
    runtime_progress: list[dict[str, Any]],
) -> None:
    by_id = {row["id"]: row for row in groups}
    tw_summary = _read_json(root / "data_tw_public/download_summary.json", {})
    tw_receipt = _read_json(root / "artifacts/data_refresh/tw_public/latest.json", {})
    tw_events = _read_json(
        root / "artifacts/data_refresh/tw_public/events/latest.json", {}
    )
    tw = by_id.get("group:tw-public")
    if tw is not None and isinstance(tw_summary, Mapping):
        total = _integer(tw_summary.get("dataset_count")) or 0
        completed = (_integer(tw_summary.get("ok_count")) or 0) + (
            _integer(tw_summary.get("up_to_date_count")) or 0
        )
        coverage_complete = tw_summary.get("coverage_complete") is True
        receipt_ok = (
            isinstance(tw_receipt, Mapping) and tw_receipt.get("status") == "ok"
        )
        tw["coverage"] = _coverage(completed, total, unit="資料集", label="完整稽核")
        tw["data_through"] = str(tw_summary.get("end_date") or "") or None
        tw["rows"] = _integer(tw_summary.get("rows_total"))
        if coverage_complete and receipt_ok and tw["freshness"]["state"] == "current":
            tw["status"] = "current"
            tw["status_label"] = "不可變快照完整且最新"
            tw["eta"] = _complete_eta("全部公開資料集缺口稽核通過並完成快照切換。")
        elif coverage_complete:
            tw["status"] = "stale"
            tw["status_label"] = "完整快照需要更新"
        event_registered = (
            _integer(tw_events.get("registered_dataset_count"))
            if isinstance(tw_events, Mapping)
            else None
        )
        event_observed = (
            _integer(tw_events.get("observed_dataset_count"))
            if isinstance(tw_events, Mapping)
            else None
        )
        event_healthy = bool(
            isinstance(tw_events, Mapping)
            and tw_events.get("status") == "ok"
            and tw_events.get("coverage_complete") is True
            and event_registered is not None
            and event_registered > 0
            and event_observed == event_registered
            and _integer(tw_events.get("failed_probe_count")) == 0
            and _integer(tw_events.get("unapplied_event_count")) == 0
        )
        tw["event_monitor"] = {
            "healthy": event_healthy,
            "status": tw_events.get("status")
            if isinstance(tw_events, Mapping)
            else None,
            "registered": event_registered,
            "observed": event_observed,
            "failed_probes": _integer(tw_events.get("failed_probe_count"))
            if isinstance(tw_events, Mapping)
            else None,
            "unapplied_events": _integer(tw_events.get("unapplied_event_count"))
            if isinstance(tw_events, Mapping)
            else None,
            "updated_at": tw_events.get("updated_at_taipei")
            if isinstance(tw_events, Mapping)
            else None,
        }
        tw["cadence"] = "來源事件每 60–300 秒；08:00/08:20/08:29 全量驗收"
        tw["update_owner"] = "來源事件監測器＋不可變快照更新器"
        if event_healthy and tw.get("status") == "current":
            tw["status_label"] = "156/156 來源事件健康，快照完整最新"
        elif not event_healthy:
            tw["status"] = "degraded"
            tw["status_label"] = "來源版本事件監測尚未全數健康"
            tw["eta"] = _unknown_eta(
                "running_unmeasured", "等待 156 項來源完成探測與事件套用。"
            )

    pipeline_by_id = {
        str(row.get("id")): row
        for row in shioaji.get("pipelines", [])
        if isinstance(row, Mapping)
    }
    for group_id, pipeline_id in {
        "group:tw-minute-train": "minute_research",
        "group:tw-minute-source-cold": "stock_minute",
        "group:tw-microstructure-train": "hft_dataset",
        "group:tw-microstructure-captures-cold": "fop_stream",
        "group:tw-futures": "futures_history",
    }.items():
        group = by_id.get(group_id)
        pipeline = pipeline_by_id.get(pipeline_id)
        if group is None or pipeline is None:
            continue
        raw_status = str(pipeline.get("status") or "unavailable")
        group["status"] = {
            "active": "updating",
            "ready": "current",
            "waiting": "waiting",
            "attention": "degraded",
        }.get(raw_status, "unavailable")
        group["status_label"] = str(pipeline.get("status_label") or raw_status)
        group["coverage"] = (
            dict(pipeline["coverage"])
            if isinstance(pipeline.get("coverage"), Mapping)
            else None
        )
        group["eta"] = (
            dict(pipeline["eta"])
            if isinstance(pipeline.get("eta"), Mapping)
            else group["eta"]
        )
        group["latest_at_utc"] = pipeline.get("latest_at_utc")
        group["warnings"] = [str(value) for value in pipeline.get("warnings", [])]
        group["detail_link"] = "../shioaji/"

    archive = openbb.get("archive")
    process = openbb.get("process")
    if isinstance(archive, Mapping):
        for group_id in ("group:openbb-compact", "group:openbb-task-shards-local"):
            group = by_id.get(group_id)
            if group is None:
                continue
            resolved = _integer(archive.get("resolved_tasks")) or 0
            total = _integer(archive.get("total_tasks")) or 0
            actionable = _integer(archive.get("actionable_unresolved_tasks")) or 0
            rates = [
                _number(row.get("recent_tasks_per_minute")) or 0.0
                for row in openbb.get("providers", [])
                if isinstance(row, Mapping)
            ]
            rate = sum(rates)
            group["coverage"] = _coverage(resolved, total, unit="任務", label="已判定")
            group["rows"] = (
                _integer(archive.get("success_rows"))
                if group_id == "group:openbb-compact"
                else None
            )
            group["data_through"] = str(archive.get("end_date") or "") or None
            source_age = _number(openbb.get("source_age_seconds"))
            latest = (
                now - timedelta(seconds=source_age) if source_age is not None else None
            )
            group["latest_at_utc"] = _iso(latest)
            group["freshness"] = _freshness(
                latest,
                now=now,
                window_seconds=15 * 60,
            )
            alive = (
                isinstance(process, Mapping) and process.get("downloader_alive") is True
            )
            audit_health = str(openbb.get("audit_health") or "unknown").lower()
            if audit_health in {"critical", "degraded"}:
                group["status"] = "degraded"
                group["status_label"] = (
                    "正在回補，但完整性稽核仍為 " + audit_health
                    if alive
                    else "完整性稽核失敗且下載程序未執行"
                )
            else:
                group["status"] = "updating" if alive else "blocked"
                group["status_label"] = "正在持續回補" if alive else "下載程序未執行"
            if actionable == 0:
                group["eta"] = _complete_eta("沒有可執行的未解任務。")
            elif rate > 0:
                seconds = int(math.ceil(actionable / rate * 60))
                group["eta"] = {
                    "state": "estimating",
                    "remaining_seconds": seconds,
                    "estimated_complete_at_utc": _iso(now + timedelta(seconds=seconds)),
                    "confidence": "low",
                    "basis": "依近期供應商總接受速率外推可執行缺口；配額與權限結果會改變 ETA。",
                }
            else:
                group["eta"] = _unknown_eta(
                    "waiting_quota",
                    "目前有效速率為零；等待配額或下一輪排程。",
                )
            group["detail"] = (
                f"已接受 {_integer(archive.get('accepted_tasks')) or 0:,}；"
                f"權威不可用 {_integer(archive.get('unavailable_tasks')) or 0:,}；"
                f"可執行缺口 {actionable:,}。"
            )
            alerts = openbb.get("alerts")
            if isinstance(alerts, list):
                group["warnings"] = [
                    str(alert.get("message"))
                    for alert in alerts
                    if isinstance(alert, Mapping) and alert.get("message")
                ][:3]
            group["detail_link"] = "../openbb/"

    active_group_services = {
        "group:tw-index-futures": ("taifex_futures",),
        "group:tw-index-derivatives-ticks": ("taifex_auxiliary",),
        "group:tw-index-options-daily": ("taifex_auxiliary",),
    }
    for group_id, service_names in active_group_services.items():
        group = by_id.get(group_id)
        if group is None:
            continue
        if not any(
            refresh_services.get(name, {}).get("active") is True
            for name in service_names
        ):
            continue
        group["status"] = "updating"
        group["status_label"] = "完整缺口更新正在執行"
        group["eta"] = _unknown_eta(
            "running_unmeasured",
            "更新正在執行；目前下載器尚未寫出足夠的批內速率，完成後會以新回執校正。",
        )

    progress_tokens = {
        "group:yahoo-market": (":us_stocks", ":crypto"),
        "group:okx": ("download:okx",),
        "group:bybit": ("download:bybit",),
        "group:binance": ("download:binance", "download:binance-history"),
        "group:forex-frankfurter": ("download:forex:frankfurter",),
    }
    progress_services = {
        "group:yahoo-market": ("registered_daily", "registered_intraday"),
        "group:okx": ("registered_intraday",),
        "group:bybit": ("registered_intraday",),
        "group:binance": ("registered_intraday",),
        "group:forex-frankfurter": ("registered_daily",),
    }
    for group_id, tokens in progress_tokens.items():
        group = by_id.get(group_id)
        if group is None or not any(
            refresh_services.get(name, {}).get("active") is True
            for name in progress_services[group_id]
        ):
            continue
        progress = _select_runtime_progress(runtime_progress, tokens=tokens)
        if progress is None:
            continue
        group["status"] = "updating"
        group["status_label"] = "完整缺口更新正在執行"
        group["coverage"] = _coverage(
            progress["current"],
            progress["total"],
            unit="項",
            label=f"執行階段 {progress['label']}",
        )
        remaining = _integer(progress.get("remaining_seconds"))
        if remaining is not None:
            group["eta"] = {
                "state": "phase_estimate",
                "remaining_seconds": remaining,
                "estimated_complete_at_utc": _iso(now + timedelta(seconds=remaining)),
                "confidence": "low",
                "basis": "下載器目前執行階段的 tqdm 吞吐率 ETA；進入下一個掃描或修復階段後會重新估算。",
            }


def _stock_stream_window(now: datetime) -> dict[str, Any]:
    local = now.astimezone(TAIPEI)
    candidates: list[tuple[datetime, datetime]] = []
    for offset in range(0, 10):
        session_date = local.date() + timedelta(days=offset)
        if session_date.weekday() >= 5:
            continue
        starts = datetime.combine(
            session_date, datetime_time(8, 45), tzinfo=TAIPEI
        )
        ends = datetime.combine(session_date, datetime_time(13, 30), tzinfo=TAIPEI)
        if ends > local:
            candidates.append((starts, ends))
    if not candidates:
        raise RuntimeError("could not resolve next stock capture window")
    starts, ends = min(candidates, key=lambda value: value[0])
    return {
        "kind": "tw_stock",
        "timezone": "Asia/Taipei",
        "schedule_label": "交易日 08:45–13:30（以實際訂閱與落盤心跳為準）",
        "state": "open" if starts <= local < ends else "waiting",
        "starts_at_utc": _iso(starts.astimezone(UTC)),
        "ends_at_utc": _iso(ends.astimezone(UTC)),
    }


def _taifex_stream_window(now: datetime) -> dict[str, Any]:
    window = next_taifex_capture_window(now.astimezone(TAIPEI))
    local = now.astimezone(TAIPEI)
    return {
        "kind": "taifex",
        "timezone": "Asia/Taipei",
        "schedule_label": (
            "交易日 08:30–13:45、14:50–次日 05:00"
            "（正常週曆；休市以實際訂閱與落盤心跳為準）"
        ),
        "session": window.session,
        "trading_date": window.trading_date.isoformat(),
        "state": "open" if window.starts_at <= local < window.stops_at else "waiting",
        "starts_at_utc": _iso(window.starts_at.astimezone(UTC)),
        "ends_at_utc": _iso(window.stops_at.astimezone(UTC)),
    }


def _next_declared_calendar(
    profile: Mapping[str, Any], now: datetime
) -> datetime | None:
    raw_clock = str(profile.get("calendar_time") or "")
    if not raw_clock:
        return None
    try:
        hour_text, minute_text = raw_clock.split(":", maxsplit=1)
        clock = datetime_time(int(hour_text), int(minute_text))
    except (TypeError, ValueError):
        return None
    local = now.astimezone(TAIPEI)
    candidate = datetime.combine(local.date(), clock, tzinfo=TAIPEI)
    if candidate <= local:
        candidate += timedelta(days=1)
    if profile.get("calendar_weekdays") is True:
        while candidate.weekday() >= 5:
            candidate += timedelta(days=1)
    return candidate.astimezone(UTC)


def _profile_for_row(row: Mapping[str, Any]) -> dict[str, Any]:
    row_id = str(row.get("id") or "")
    shioaji_profiles: dict[str, dict[str, Any]] = {
        "shioaji:fop_stream": {
            **_AUTOMATION_PROFILES["group:tw-microstructure-captures-cold"],
            "service_keys": ("shioaji_fop_stream",),
            "stream_kind": "taifex",
        },
        "shioaji:top200_stream": {
            **_AUTOMATION_PROFILES["group:tw-microstructure-captures-cold"],
            "service_keys": ("shioaji_stock_stream",),
            "stream_kind": "tw_stock",
        },
        "shioaji:hft_dataset": _AUTOMATION_PROFILES[
            "group:tw-microstructure-train"
        ],
        "shioaji:stock_minute": _AUTOMATION_PROFILES["group:tw-minute-source-cold"],
        "shioaji:minute_research": _AUTOMATION_PROFILES["group:tw-minute-train"],
        "shioaji:stock_daily": _AUTOMATION_PROFILES["group:tw-minute-train"],
        "shioaji:futures_history": _AUTOMATION_PROFILES["group:tw-futures"],
        "shioaji:contract_catalog": _AUTOMATION_PROFILES["group:tw-futures"],
        "shioaji:on_demand_snapshots": {
            "mode": "on_demand",
            "service_keys": (),
            "schedule_label": "策略需要報價時逐次查詢；不持續輪詢",
            "active_means_running": False,
        },
    }
    if row_id in shioaji_profiles:
        return dict(shioaji_profiles[row_id])
    parent_id = str(row.get("parent_id") or row_id)
    profile = _AUTOMATION_PROFILES.get(parent_id)
    if profile is None:
        return {
            "mode": "not_configured",
            "service_keys": (),
            "schedule_label": "尚未註冊自動更新排程",
            "active_means_running": False,
        }
    return dict(profile)


def _automation_for_row(
    row: Mapping[str, Any],
    *,
    now: datetime,
    refresh_services: Mapping[str, Mapping[str, Any]],
) -> dict[str, Any]:
    profile = _profile_for_row(row)
    keys = tuple(str(value) for value in profile.get("service_keys", ()))
    states = [
        refresh_services[key]
        for key in keys
        if isinstance(refresh_services.get(key), Mapping)
    ]
    next_runs = [
        parsed
        for state in states
        if (parsed := _parse_time(state.get("next_run_at_utc"))) is not None
    ]
    last_triggers = [
        parsed
        for state in states
        if (parsed := _parse_time(state.get("last_trigger_at_utc"))) is not None
    ]
    starts = [
        parsed
        for state in states
        if (parsed := _parse_time(state.get("started_at_utc"))) is not None
    ]
    declared_next = _next_declared_calendar(profile, now)
    if not next_runs and declared_next is not None:
        next_runs.append(declared_next)
    next_run_basis = (
        "systemd_timer"
        if any(state.get("next_run_at_utc") for state in states)
        else "declared_calendar"
        if declared_next is not None
        else "contract_only"
    )
    active = any(state.get("active") is True for state in states)
    active_means_running = profile.get("active_means_running", True) is True
    eta_state = str((row.get("eta") or {}).get("state") or "unknown")
    row_has_live_work = str(row.get("status") or "") == "updating" or eta_state in {
        "estimating",
        "warming_up",
        "running_unmeasured",
        "phase_estimate",
    }
    # An umbrella service can be active while a different provider stage runs.
    # Require endpoint-level progress/ETA evidence before calling this row active.
    job_running = active and active_means_running and row_has_live_work
    stream_kind = str(profile.get("stream_kind") or "")
    stream_window = None
    if stream_kind == "taifex":
        stream_window = _taifex_stream_window(now)
    elif stream_kind == "tw_stock":
        stream_window = _stock_stream_window(now)
    elif stream_kind == "mixed_tw":
        taifex = _taifex_stream_window(now)
        stock = _stock_stream_window(now)
        candidates = [taifex, stock]
        open_windows = [item for item in candidates if item["state"] == "open"]
        selected = min(
            open_windows or candidates,
            key=lambda item: str(item.get("starts_at_utc") or ""),
        )
        stream_window = {
            **selected,
            "kind": "mixed_tw",
            "schedule_label": (
                "台股 08:45–13:30；TAIFEX 08:30–13:45、"
                "14:50–次日 05:00（正常週曆）"
            ),
        }
    eligible = row.get("automation_eligible", True) is True
    mode = str(profile.get("mode") or "not_configured")
    automatic = eligible and mode not in {"frozen", "not_configured", "on_demand"}
    schedule_label = str(
        profile.get("schedule_label") or row.get("cadence") or "未指定"
    )
    if not eligible and mode not in {"frozen", "on_demand"}:
        schedule_label = "未接入可執行自動更新；父群組排程不代表此端點"
    if not eligible and mode not in {"frozen", "on_demand"}:
        schedule_state = "not_configured"
    elif mode == "stream":
        schedule_state = (
            "stream_window_open"
            if stream_window and stream_window.get("state") == "open"
            else "waiting_stream_window"
        )
    elif job_running:
        schedule_state = "running"
    elif next_runs:
        schedule_state = "scheduled"
    elif mode in {"continuous_backfill", "quota_backfill"} and active:
        schedule_state = "running_or_waiting_quota"
    elif mode == "interval_after_completion":
        schedule_state = "after_previous_completion"
    elif mode == "on_demand":
        schedule_state = "on_demand"
    elif mode == "frozen":
        schedule_state = "not_applicable"
    elif mode == "not_configured":
        schedule_state = "not_configured"
    else:
        schedule_state = "schedule_declared"
    evidence = []
    if states:
        evidence.append("systemd_service")
    if any(state.get("timer_active") is True for state in states):
        evidence.append("systemd_timer")
    if row.get("latest_at_utc"):
        evidence.append("receipt_or_manifest")
    if not evidence:
        evidence.append("registry_only")
    if next_run_basis == "declared_calendar":
        evidence.append("declared_calendar")
    return {
        "mode": mode,
        "automatic_update": automatic,
        "schedule_state": schedule_state,
        "schedule_label": schedule_label,
        "service_keys": list(keys),
        "service_active": active,
        "job_running": job_running,
        "next_run_at_utc": _iso(min(next_runs)) if next_runs else None,
        "next_run_basis": next_run_basis,
        "last_trigger_at_utc": _iso(max(last_triggers)) if last_triggers else None,
        "last_started_at_utc": _iso(max(starts)) if starts else None,
        "stream_window": stream_window,
        "evidence": evidence,
    }


def _operation_state(
    row: Mapping[str, Any], automation: Mapping[str, Any]
) -> tuple[str, str, str]:
    raw_status = str(row.get("status") or "unavailable")
    eta_state = str((row.get("eta") or {}).get("state") or "unknown")
    mode = str(automation.get("mode") or "not_configured")
    stream_window = automation.get("stream_window")
    freshness_age = _number((row.get("freshness") or {}).get("age_seconds"))
    recent_stream_heartbeat = freshness_age is not None and freshness_age <= 10 * 60

    if raw_status == "deferred":
        return "complete", "deferred", str(
            row.get("status_label") or "依目前資料取得範圍延後"
        )

    if mode == "stream":
        window_open = (
            isinstance(stream_window, Mapping)
            and stream_window.get("state") == "open"
        )
        if raw_status in {"blocked", "unavailable", "degraded"}:
            return "unable", "blocked", "串流來源缺少可用的服務或落盤證據"
        if window_open and raw_status in {"updating", "current"} and recent_stream_heartbeat:
            return "streaming", "streaming", "交易時窗內且最近十分鐘有落盤心跳"
        return "catching_up", "waiting_stream_window", "目前未觀測到有效串流；等待下一個時窗或新心跳"

    actively_working = automation.get("job_running") is True or eta_state in {
        "estimating",
        "warming_up",
        "running_unmeasured",
        "phase_estimate",
    }
    if raw_status in {"blocked", "unavailable"}:
        return "unable", "blocked", str(row.get("status_label") or "來源不可用")
    if raw_status == "degraded":
        if actively_working:
            return "catching_up", "running", "正在修復已知缺口"
        return "unable", "failed", str(row.get("status_label") or "完整性稽核未通過")
    if mode == "on_demand":
        return "complete", "on_demand", "端點按需逐次完成，沒有常駐下載佇列"
    if row.get("automation_eligible", True) is not True:
        if raw_status in {"current", "complete", "legacy"}:
            return "complete", "not_applicable", "封存或按契約不需要持續更新"
        return "unable", "not_configured", "來源已登錄，但尚未具備可執行的自動更新管線"
    if raw_status == "updating" or actively_working:
        return "catching_up", "running", "更新工作執行中，尚未到最新"
    if raw_status in {"waiting", "stale"} or eta_state in {
        "waiting_quota",
        "waiting_schedule",
    }:
        execution = (
            "scheduled"
            if automation.get("schedule_state")
            in {"scheduled", "after_previous_completion", "schedule_declared"}
            else "waiting_quota"
            if eta_state == "waiting_quota"
            else "waiting"
        )
        return "catching_up", execution, str(row.get("status_label") or "等待更新")
    if raw_status in {"current", "complete", "legacy"}:
        return "complete", "idle_current", "最新可驗證批次已完成"
    return "unable", "unknown", "缺少足夠狀態證據，無法判定會自動完成"


def _next_data_date(value: Any) -> str | None:
    """Return the next calendar data date for a source-backed date value."""

    text = str(value or "").strip()
    match = re.match(r"^(\d{4}-\d{2}-\d{2})(?:$|T)", text)
    if match is None:
        return None
    try:
        parsed = date.fromisoformat(match.group(1))
    except ValueError:
        return None
    return (parsed + timedelta(days=1)).isoformat()


def _publication_for_row(
    row: Mapping[str, Any],
    *,
    automation: Mapping[str, Any],
    hint: Mapping[str, Any] | None,
) -> dict[str, Any]:
    """Describe upstream publication separately from our acquisition timer."""

    explicit = dict(hint) if isinstance(hint, Mapping) else {}
    mode = str(automation.get("mode") or "not_configured")
    cadence = str(row.get("cadence") or "依來源更新")
    scope = str(row.get("scope") or "")
    if scope == "credential_gate":
        schedule_kind = "not_applicable"
        schedule_label = "憑證狀態，不是公開資料發布端點"
        basis = "此列只驗證憑證是否已設定，不代表任何上游資料發布。"
    elif explicit:
        schedule_kind = str(explicit.get("schedule_kind") or "source_contract")
        schedule_label = str(
            explicit.get("schedule_label")
            or "來源未承諾固定發布時刻；以實際偵測為準"
        )
        basis = str(
            explicit.get("basis")
            or "發布時間來自來源契約或版本偵測收據。"
        )
    elif mode == "stream":
        schedule_kind = "continuous"
        schedule_label = f"連續發布；{cadence}"
        basis = "串流資料沒有單一發布時刻；最近落盤心跳代表實際觀測。"
    elif mode == "on_demand":
        schedule_kind = "on_demand"
        schedule_label = "按需產生，沒有固定發布時刻"
        basis = "只有呼叫端點時才產生資料。"
    elif mode == "frozen":
        schedule_kind = "not_applicable"
        schedule_label = "凍結封存，不再發布"
        basis = "封存資料不再追蹤下一次發布。"
    else:
        schedule_kind = "cadence_only"
        schedule_label = f"來源未承諾固定時刻；發布頻率：{cadence}"
        basis = (
            "目前只有來源 cadence 或我方取得契約；最近觀測時間不冒充上游官方發布時間。"
        )
    return {
        "schedule_kind": schedule_kind,
        "schedule_label": schedule_label,
        "exact_time_declared": explicit.get("exact_time_declared") is True,
        "probe_boundaries_taipei": list(
            explicit.get("probe_boundaries_taipei") or []
        ),
        "detected_at_utc": explicit.get("detected_at_utc"),
        "last_checked_at_utc": explicit.get("last_checked_at_utc"),
        "applied_at_utc": explicit.get("applied_at_utc"),
        "next_check_at_utc": explicit.get("next_check_at_utc"),
        "observed_at_utc": row.get("latest_at_utc"),
        "basis": basis,
        "receipt_phases": list(explicit.get("receipt_phases") or []),
        "acquisition_schedule_label": automation.get("schedule_label"),
        "next_acquisition_at_utc": automation.get("next_run_at_utc"),
    }


def _acquisition_progress(
    row: Mapping[str, Any],
    *,
    operation: str,
    execution: str,
    publication: Mapping[str, Any],
) -> dict[str, Any]:
    """Normalize per-source progress while preserving unknown denominators."""

    coverage = row.get("coverage")
    coverage = coverage if isinstance(coverage, Mapping) else {}
    ratio = _number(coverage.get("ratio"))
    if ratio is not None:
        ratio = min(1.0, max(0.0, ratio))
    current = _integer(coverage.get("current"))
    total = _integer(coverage.get("total"))
    data_through = str(row.get("data_through") or "").strip() or None
    preparing_for_date = _next_data_date(data_through)
    first_data_observed = bool(
        data_through
        or (current is not None and current > 0)
        or ((_integer(row.get("rows")) or 0) > 0)
        or operation == "streaming"
    )
    up_to_date = operation in {"complete", "streaming"} and execution != "deferred"
    coverage_complete = ratio >= 1.0 if ratio is not None else False
    batch_complete = operation == "complete" or (
        coverage_complete and execution not in {"running", "streaming"}
    )
    if operation == "streaming":
        state = "streaming"
        label = "持續取得中；串流沒有總完工日"
    elif execution == "deferred":
        state = "deferred"
        label = "已延後；不列入主動取得範圍"
    elif up_to_date:
        state = "complete"
        label = "取得完成且已到最新"
    elif operation == "unable":
        state = "blocked"
        label = "取得未完成；目前受阻"
    elif batch_complete and preparing_for_date:
        state = "preparing_next_date"
        label = f"本批完成；準備下一資料日 {preparing_for_date}"
    elif first_data_observed and preparing_for_date:
        state = "acquiring"
        label = f"首筆已到；取得中並準備下一資料日 {preparing_for_date}"
    elif first_data_observed:
        state = "acquiring"
        label = "已收到資料；取得中"
    else:
        state = "waiting"
        label = "尚未收到首筆資料"

    progress_basis = str(coverage.get("label") or "").strip()
    eta = row.get("eta")
    eta = eta if isinstance(eta, Mapping) else {}
    completed_receipt = bool(row.get("latest_at_utc")) and str(
        eta.get("state") or ""
    ) == "complete"
    if ratio is None and up_to_date and completed_receipt:
        # A timestamped, high-confidence completed receipt is a valid binary
        # denominator.  An on-demand contract alone is not.
        ratio = 1.0
        current = 1
        total = 1
        unit = "完成收據"
        progress_basis = "最新完成收據"
    else:
        unit = str(coverage.get("unit") or "").strip() or None
        if ratio is None:
            progress_basis = (
                "已收到首筆，但來源未提供可靠總量"
                if first_data_observed
                else "來源未提供可靠分子／分母"
            )
    first_data_at = publication.get("applied_at_utc") or None
    return {
        "state": state,
        "label": label,
        "current": current,
        "total": total,
        "ratio": ratio,
        "unit": unit,
        "basis": progress_basis,
        "first_data_observed": first_data_observed,
        "first_data_at_utc": first_data_at,
        "data_through": data_through,
        "preparing_for_date": preparing_for_date,
        "coverage_complete": coverage_complete,
        "batch_complete": batch_complete,
        "up_to_date": up_to_date,
    }


def _row_sort_key(row: Mapping[str, Any]) -> tuple[Any, ...]:
    operation = str(row.get("operation_state") or "unable")
    execution_order = {
        "running": 0,
        "streaming": 0,
        "waiting_stream_window": 1,
        "scheduled": 2,
        "waiting_quota": 3,
        "waiting": 4,
        "idle_current": 5,
        "on_demand": 6,
        "deferred": 7,
        "not_applicable": 8,
        "not_configured": 9,
        "failed": 10,
        "blocked": 11,
        "unknown": 12,
    }
    eta = _number((row.get("eta") or {}).get("remaining_seconds"))
    next_run = _parse_time((row.get("automation") or {}).get("next_run_at_utc"))
    return (
        _OPERATION_ORDER.get(operation, 99),
        execution_order.get(str(row.get("execution_state") or "unknown"), 99),
        eta if eta is not None else math.inf,
        next_run.timestamp() if next_run is not None else math.inf,
        str(row.get("provider") or "").casefold(),
        str(row.get("title") or "").casefold(),
        str(row.get("id") or ""),
    )


def _enrich_and_sort_rows(
    rows: Iterable[dict[str, Any]],
    *,
    now: datetime,
    refresh_services: Mapping[str, Mapping[str, Any]],
) -> list[dict[str, Any]]:
    enriched: list[dict[str, Any]] = []
    for original in rows:
        row = dict(original)
        publication_hint = row.pop("_publication_hint", None)
        automation = _automation_for_row(
            row, now=now, refresh_services=refresh_services
        )
        operation, execution, reason = _operation_state(row, automation)
        publication = _publication_for_row(
            row,
            automation=automation,
            hint=(
                publication_hint
                if isinstance(publication_hint, Mapping)
                else None
            ),
        )
        row["endpoint_id"] = str(row.get("id") or "")
        row["operation_state"] = operation
        row["operation_label"] = _OPERATION_LABELS[operation]
        row["operation_rank"] = _OPERATION_ORDER[operation]
        row["execution_state"] = execution
        row["operation_reason"] = reason
        row["in_active_scope"] = execution != "deferred"
        row["is_latest"] = operation == "streaming" or (
            operation == "complete" and execution != "deferred"
        )
        row["last_verified_at_utc"] = row.get("latest_at_utc")
        row["automation"] = automation
        row["publication"] = publication
        row["acquisition_progress"] = _acquisition_progress(
            row,
            operation=operation,
            execution=execution,
            publication=publication,
        )
        enriched.append(row)
    enriched.sort(key=_row_sort_key)
    for index, row in enumerate(enriched, start=1):
        row["sort_index"] = index
    return enriched


def _provider_summaries(rows: Iterable[Mapping[str, Any]]) -> list[dict[str, Any]]:
    buckets: dict[str, list[Mapping[str, Any]]] = {}
    for row in rows:
        buckets.setdefault(str(row.get("provider") or "其他"), []).append(row)
    output = []
    for provider, items in buckets.items():
        counts: dict[str, int] = {}
        operation_counts: dict[str, int] = {}
        for item in items:
            status = str(item.get("status") or "unavailable")
            counts[status] = counts.get(status, 0) + 1
            operation = str(item.get("operation_state") or "unable")
            operation_counts[operation] = operation_counts.get(operation, 0) + 1
        worst = max(counts, key=lambda value: _STATUS_PRIORITY.get(value, 99))
        output.append(
            {
                "provider": provider,
                "status": worst,
                "registered": len(items),
                "status_counts": counts,
                "operation_state_counts": operation_counts,
            }
        )
    return sorted(output, key=lambda row: (-row["registered"], row["provider"].lower()))


def build_data_monitor_public_status(
    repo_root: Path,
    *,
    now: datetime | None = None,
    refresh_services: Mapping[str, Mapping[str, Any]] | None = None,
    shioaji_status: Mapping[str, Any] | None = None,
    openbb_status: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Build the complete public registry and current monitor projection."""

    root = Path(repo_root)
    observed = (now or datetime.now(UTC)).astimezone(UTC)
    registry = _read_json(root / "configs/data_sync/packed_datasets.json", {})
    configs = registry.get("datasets", []) if isinstance(registry, Mapping) else []
    groups = [
        _generic_group(root, config, now=observed)
        for config in configs
        if isinstance(config, Mapping)
    ]
    shioaji = (
        dict(shioaji_status)
        if isinstance(shioaji_status, Mapping)
        else build_shioaji_public_status(root)
    )
    openbb = (
        dict(openbb_status)
        if isinstance(openbb_status, Mapping)
        else build_openbb_public_status(root)
    )
    service_states = (
        _refresh_service_states(
            snapshot_path=(
                root / "artifacts/live/data_monitor/refresh_services.json"
            ),
            now=observed,
            prefer_snapshot=True,
        )
        if refresh_services is None
        else {str(key): dict(value) for key, value in refresh_services.items()}
    )
    runtime_progress = _runtime_progress(root)
    _specialize_groups(
        groups,
        shioaji=shioaji,
        openbb=openbb,
        root=root,
        now=observed,
        refresh_services=service_states,
        runtime_progress=runtime_progress,
    )
    history_logical = _crypto_history_sources(root, now=observed)
    logical = (
        _tw_public_sources(root, now=observed)
        + _shioaji_sources(shioaji, now=observed)
        + _openbb_sources(openbb, now=observed)
        + _crypto_feature_sources(root, now=observed)
        + _credential_registry_sources(root, now=observed)
        + _product_granularity_sources(root, now=observed)
        + _crypto_acquisition_sources(root, now=observed)
        + _free_public_registry_sources(root, now=observed)
        + history_logical
    )
    _rollup_crypto_history_groups(groups, history_logical)
    rows = _enrich_and_sort_rows(
        groups + logical,
        now=observed,
        refresh_services=service_states,
    )
    groups = [row for row in rows if row.get("scope") == "storage_group"]
    logical = [row for row in rows if row.get("scope") != "storage_group"]
    status_counts: dict[str, int] = {}
    operation_counts = {state: 0 for state in _OPERATION_ORDER}
    for row in rows:
        status = str(row.get("status") or "unavailable")
        status_counts[status] = status_counts.get(status, 0) + 1
        operation = str(row.get("operation_state") or "unable")
        operation_counts[operation] = operation_counts.get(operation, 0) + 1
    attention = operation_counts["unable"]
    healthy = len(rows) - attention
    worst_status = max(
        (str(row.get("status") or "unavailable") for row in rows),
        key=lambda value: _STATUS_PRIORITY.get(value, 99),
        default="unavailable",
    )
    if attention:
        health = (
            "critical" if worst_status in {"blocked", "unavailable"} else "degraded"
        )
    elif operation_counts["catching_up"]:
        health = "updating"
    else:
        health = "active"
    known_rows = sum(
        value
        for value in (_integer(row.get("rows")) for row in groups)
        if value is not None
    )
    timing_defined = sum(
        bool(str((row.get("automation") or {}).get("schedule_label") or "").strip())
        for row in rows
    )
    automatic = sum(
        (row.get("automation") or {}).get("automatic_update") is True for row in rows
    )
    next_run_known = sum(
        bool((row.get("automation") or {}).get("next_run_at_utc")) for row in rows
    )
    systemd_next_run_known = sum(
        (row.get("automation") or {}).get("next_run_basis") == "systemd_timer"
        for row in rows
    )
    publication_detected = sum(
        bool((row.get("publication") or {}).get("detected_at_utc")) for row in rows
    )
    publication_exact = sum(
        (row.get("publication") or {}).get("exact_time_declared") is True
        for row in rows
    )
    progress_denominator_known = sum(
        _number((row.get("acquisition_progress") or {}).get("ratio")) is not None
        for row in rows
    )
    first_data_observed = sum(
        (row.get("acquisition_progress") or {}).get("first_data_observed") is True
        for row in rows
    )
    preparing_next_date = sum(
        bool((row.get("acquisition_progress") or {}).get("preparing_for_date"))
        for row in rows
    )
    deferred_count = sum(row.get("execution_state") == "deferred" for row in rows)
    active_scope_count = len(rows) - deferred_count
    completed_in_scope = operation_counts["complete"] - deferred_count
    return {
        "schema_version": DATA_MONITOR_SCHEMA_VERSION,
        "generated_at_utc": _iso(observed),
        "health": health,
        "read_only": True,
        "production_control_possible": False,
        "summary": {
            "registered_items": len(rows),
            "storage_groups": len(groups),
            "logical_sources": len(logical),
            "product_granularities": sum(
                row.get("scope") == "product_granularity" for row in logical
            ),
            "credential_gates": sum(
                row.get("scope") == "credential_gate" for row in logical
            ),
            "crypto_fact_families": sum(
                row.get("scope") == "crypto_fact_family" for row in logical
            ),
            "healthy_or_progressing": healthy,
            "attention_required": attention,
            "catching_up": operation_counts["catching_up"],
            "streaming": operation_counts["streaming"],
            "completed": completed_in_scope,
            "deferred": deferred_count,
            "active_scope_items": active_scope_count,
            "unable": operation_counts["unable"],
            "known_group_rows": known_rows,
            "status_counts": status_counts,
            "operation_state_counts": operation_counts,
            "source_level_ratio": (
                (completed_in_scope + operation_counts["streaming"])
                / active_scope_count
                if active_scope_count
                else 0.0
            ),
        },
        "endpoint_inventory": {
            "total": len(rows),
            "active_scope_total": active_scope_count,
            "deferred": deferred_count,
            "ordered_states": [
                {"state": state, "label": _OPERATION_LABELS[state], "rank": rank}
                for state, rank in _OPERATION_ORDER.items()
            ],
            "state_counts": operation_counts,
            "timing_defined": timing_defined,
            "timing_coverage_ratio": timing_defined / len(rows) if rows else 1.0,
            "automatic_update_registered": automatic,
            "explicit_nonautomatic": len(rows) - automatic,
            "next_planned_at_known": next_run_known,
            "systemd_exact_next_run_known": systemd_next_run_known,
            "declared_calendar_next_run_known": (
                next_run_known - systemd_next_run_known
            ),
            "publication_detected": publication_detected,
            "publication_exact_time_declared": publication_exact,
            "publication_cadence_or_boundary_only": len(rows) - publication_exact,
            "progress_denominator_known": progress_denominator_known,
            "first_data_observed": first_data_observed,
            "preparing_next_data_date": preparing_next_date,
            "sort_contract": (
                "operation_rank, execution_state, measured_eta, next_run, "
                "provider, title, endpoint_id"
            ),
        },
        "provider_summaries": _provider_summaries(rows),
        "refresh_services": service_states,
        "active_progress": runtime_progress,
        "groups": groups,
        "sources": rows,
        "definitions": {
            "freshness": "最新回執是否落在各來源允許的更新時間窗。",
            "completeness": "該來源以自己的日期、標的、任務或資料集單位稽核；不同單位不相加。",
            "eta": "只有執行中且存在有效吞吐率時才估算；配額、休市或零速率會明示未知。",
            "operation_state": (
                "固定排序為：正在抓／還沒到最新、正在串流、已完成／已到最新、"
                "無法完成；原始下載器狀態仍保留供稽核。"
            ),
            "streaming": (
                "服務常駐不等於正在串流；必須同時位於交易時窗且最近十分鐘有落盤心跳。"
            ),
            "schedule": (
                "next_run_at_utc 取自 systemd timer；完成後間隔、配額回補、"
                "盤別與按需端點則明示其排程契約，不捏造精確時間。"
            ),
            "publication": (
                "發布時間與我方取得排程分開；只有來源明示或版本變更收據才標為發布證據，"
                "掃描邊界與 cadence 不冒充每個端點的官方 SLA。"
            ),
            "acquisition_progress": (
                "進度優先取完整度 receipt/manifest 的分子分母；分母未知時保持未知。"
                "首筆資料到達即顯示下一個日曆資料日，但完整性未通過前不標示完成。"
            ),
            "source_level_progress": "面板監控項目的狀態比例，不是資料列數完成率。",
            "realtime_boundary": "即時 Tick／BidAsk 是連續流，沒有總完工日；歷史 Tick 不能重建未曾擷取的五檔委託簿。",
            "tw_public_boundary": "臺灣官方資料只透過完整稽核後的不可變快照切換，不直接修改已發佈版本。",
        },
    }


__all__ = ["DATA_MONITOR_SCHEMA_VERSION", "build_data_monitor_public_status"]
